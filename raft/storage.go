// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {			//作用是存储当前节点收到的Entry记录
	// InitialState returns the saved HardState and ConfState information.
	// 返回Storage中记录的状态信息，返回的是HardState实例和ConfState实例。Raft协议提到，集群中每个节点都需要保持一些必须的基本信息，在etcd中将其
	// 封装成HardState，其中主要封装了当前任期号(Term字段)、当前节点在该任期中将选票投给了哪个节点(Vote字段)、已提交Entry记录的位置(Commit字段，即
	// 最后一条已提交记录的索引值)；ConfState中封装了当前集群中所有节点的ID(Nodes字段)
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).    在Storage中记录了当前节点的所有Entry记录，Entries方法返回指定范围的Entry记录
	// MaxSize limits the total size of the log entries returned, but  ([lo,hi))，第三个参数(maxSize)限定了返回的Entry集合的字节数上限
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range           查询指定Index对应的Entry的Term值
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.               返回Storage中记录的第一条Entry的索引值(Index)
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is             该方法返回Storage中记录的第一条Entry的索引值(Index)，在该Entry之前的
	// possibly available via Entries (older entries have been incorporated	   所有Entry都已经被包含进了最近的一次SnapShot中
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.                              返回最近一次生成的快照数据
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an   MemoryStorage是etcd-raft模块为Storage接口提供的一个实现。
// in-memory array.
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState			//维护上述状态信息
	snapshot  pb.Snapshot			//快照数据
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry					//所有的Entry记录
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]pb.Entry, 1),
	}
}
// 该方法会直接返回hardState字段中记录的HardState实例并使用快照的元数据中记录信息创建ConfState实例返回。
// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.          	负责查询指定范围的Entry
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()											//加锁和解锁的相关操作
	defer ms.Unlock()
	offset := ms.ents[0].Index							//如果待查询的最小Index值(参数lo)小于FirstIndex，则直接抛出异常
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {							//如果待查询的最大Index值(参数hi)大于LastIndex，则直接抛出异常
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.						如果MemoryStorage.ents只包含一条Entry，则其为空Entry，直接抛出异常
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]				//获取lo~hi之间的Entry，并返回
	return limitSize(ents, maxSize), nil				//限制返回Entry切片的总字节大小
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.			返回ents数组中最后一个元素的Index字段值
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.         返回ents数组中第一个元素的Index字段值
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.				上层模块通过该方法获取SnapShot
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with  当MemoryStorage需要更新快照数据时，会调用该方法将SnapShot实例保存到MemoryStorage中
// those of the given snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()						//加锁同步,MemoryStorage实现了sync.Mutex
	defer ms.Unlock()				//方法结束后，释放锁

	//handle check for old snapshot being applied 	通过快照的元数据比较当前MemoryStorage中记录的SnapShot与待处理的SnapShot数据的新旧程序
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {		//比较两个pb.SnapShot所包含的最后一条记录的Index值
		return ErrSnapOutOfDate	//若待处理SnapShot数据比较旧，则直接抛出异常
	}

	ms.snapshot = snap				//更新MemoryStorage.snapshot字段
	//重置MemoryStorage.ents字段，此时在ents中只有一个空的Entry实例
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and   随着系统的运行，MemoryStorage.ents中保存的Entry记录会不断增加，为了
// can be used to reconstruct the state at that point.                          减小内存的压力，定期创建快照来记录当前节点的状态并压缩MemoryStorage.ents
// If any configuration changes have been made since the last compaction,       数据的空间是非常必要的，这样可以降低内存使用。
// the result of the last ApplyConfChange must be passed in.
// 该方法的参数:i=>新建SnapShot包含的最大的索引值，cs=>当前集群的状态;data=>新建snpshot的具体数据
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()									//加锁、解锁相关代码
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {		//边界检查，i必须小于等于当前SnapShot包含的最大Index值
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {						//i小于MemoryStorage的LastIndex值，否则抛出异常
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i				//更新MemoryStorage.snapshot的元数据
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data						//更新具体的快照数据
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.                      新建SnapShot之后，一般会调用该方法将MemoryStorage.ents中指定索引
// It is the application's responsibility to not attempt to compact an index	之前的Entry记录全部抛弃，从而实现压缩MemoryStorage的目的。
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()									//加锁、解锁操作
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {					//边界检测，firstIndex < compactIndex < lastIndex
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset					//compackIndex对应Entry的下标
	//创建新的切片，用来存储compackIndex之后的Entry
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	//将compactIndex之后的Entry拷贝到ents中，并更新MemoryStorage.ents字段
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.            设置完快照数据之后，就可以开始向MemoryStorage中追加Entry记录
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {				//检测entries切片的长度
		return nil
	}

	ms.Lock()							//加锁/解锁的逻辑
	defer ms.Unlock()

	first := ms.firstIndex()			//获取当前MemoryStorage的FirstIndex值
	//获取待添加的最后一条Entry的Index值
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {					//entries切片中所有的Entry都已经过时，无需添加任何Entry
		return nil
	}
	// truncate compacted entries		//first之前的Entry已经记入SnapShot中，不应该再记录到ents中，所以这部分Entry截掉
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	//计算entries切片中第一条可用的Entry与first之间的差距
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:	//参考images中MemeoyStore_Append原理.png中的场景A
		//保留MemoryStorage.ents中first~offset的部分，offset之后的部分被抛弃
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		//然后将待追加的Entry追加到MemoryStorage.ents中
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset://参考images中MemeoyStore_Append原理.png中的场景B
		//直接将待追加的日志记录(entries)追加到MemoryStorage中
		ms.ents = append(ms.ents, entries...)
	default:
		//异常处理
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
