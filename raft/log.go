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
	"fmt"
	"log"

	pb "github.com/coreos/etcd/raft/raftpb"
)
//Raft协议中日志复制部分的核心就是在集群中各个节点之间完成日志的复制，因此使用该结构体来管理节点上的日志。
type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage		//实际上就是MemoryStorage实例，其中存储了快照数据及快照之后的Entry记录

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	unstable unstable	//用于存储未写入Storage的快照数据及Entry记录

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64	//已提交的位置，即已提交的Entry记录中最大的索引值
	// applied is the highest log position that the application has		已应用的位置，即已应用的Entry记录中最大的索引值。其他committed和applied
	// been instructed to apply to its state machine.					之间始终满足committed<=applied这个不等式关系
	// Invariant: applied <= committed
	applied uint64

	logger Logger
}

// newLog returns log using the given storage. It recovers the log to the state
// that it just commits and applies the latest snapshot.
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{						//创建raftLog实例，并初始化storage字段
		storage: storage,
		logger:  logger,
	}
	firstIndex, err := storage.FirstIndex()	//获取Storage中的第一条Entry和最后一条Entry的索引值
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	log.unstable.offset = lastIndex + 1		//初始化unstable.offset
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.   初始化committed、applied字段
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}
// 该方法的参数:index=>MsgApp消息携带的第一条Entry的Index值;logTerm=>MsgAPP消息的logTerm字段，通过消息中携带的该Term值与当前节点记录的Term进行比较,
// 就可以判断消息是否为过时的消息;commited=>MsgApp消息的Commit字段，Leader节点通过该字段通过Follower节点当前已提交Entry的位置；ents=>MsgAPP消息中
// 携带的Entry记录，即待追加到raftLog中的Entry记录；
// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {		//该方法完成追加Entry记录
	if l.matchTerm(index, logTerm) {			//调用matchTerm方法检测MsgAPP消息的Index字段及LogTerm字段是否合法
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		//raftLog会根据findConflict方法的返回值决定如何进行后续的追加操作
		switch {
		case ci == 0:							//findConflict方法返回0时，表示raftLog中已经包含了所有待追加的Entry记录，不必进行任何追加操作
		case ci <= l.committed:					//如果出现冲突的位置是已提交的记录，则输出异常日志并终止整个程序
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:								//如果冲突位置是未提交的部分
			offset := index + 1
			l.append(ents[ci-offset:]...)		//将ents中未发生冲突的部分追加到raftLog中
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {										//合法性检测
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)						//调用unstable.truncateAndAppend方法将Entry记录追加到unstable中
	return l.lastIndex()									//返回raftLog最后一条日志记录的索引
}

// findConflict finds the index of the conflict.                           该方法遍历待追加的Entry集合，查找是否与raftLog中已有的Entry发生冲突
// It returns the first pair of conflicting entries between the existing   (Index相同但Term不同)
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {					//遍历全部待追加的Entry，判断raftLog中是否存在冲突的Entry记录
		if !l.matchTerm(ne.Index, ne.Term) {	//查找冲突的Entry记录
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index					//返回冲突记录的索引值
		}
	}
	return 0									//若没有发生冲突Entry，则返回0
}

func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}
// 当上层模块需要从raftLog获取Entry记录进行处理时，会先调用hasNextEnts方法检测是否有待应用的记录，然后调用nextEnts方法将已提交未应用的Entry记录
// 返回给上层模块处理。
// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())					//获取当前已经应用记录的位置
	if l.committed+1 > off {								//是否存在已提交未应用的Entry记录
		//获取全部已提交且未应用的Entry记录并返回
		ents, err := l.slice(off, l.committed+1, noLimit)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())			//获取当前已经应用记录的位置
	return l.committed+1 > off						//是否存在已提交且未应用的Entry记录
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) firstIndex() uint64 {				//返回raftLog中第一条Entry记录的索引值
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {				//返回raftLog中最后一条Entry记录的索引值
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}
//通过该方法更新raftLog.committed字段。
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {			//raftLog.committed字段只能后移，不能前移
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit			//更新committed字段
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}
//该方法会先去unstable中查找相应的Entry记录，若查找不到，则再去storage中查找
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {		//检测指定的索引值的合法性
		// TODO: return an error instead?
		return 0, nil
	}

	if t, ok := l.unstable.maybeTerm(i); ok {		//尝试从unstable中获取对应的Entry记录并返回其Term值
		return t, nil
	}

	t, err := l.storage.Term(i)						//尝试从storage中获取对应的Entry记录并返回其Term值
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err								//异常场景返回0
	}
	panic(err) // TODO(bdarnell)
}
//在Leader节点向Follower节点发送MsgAPP消息时，需要根据Follower节点的日志复制(NextIndex和MatchIndex)情况决定发送的Entry记录，此时需要调用该方法获取
//指定的Entry记录；
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {							//检测指定的Entry记录索引值是否超出了raftLog的范围
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)	//获取指定范围的Entry记录
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date  Follower节点在接收到Candidate节点的选举请求之后，会通过比较Candidate
// by comparing the index and term of the last entries in the existing logs.   节点的本地日志与自身本地日志的新旧程度，从而决定是否投票。该方法用于
// If the logs have last entries with different terms, then the log with the   比较日志的新旧程序；
// later term is more up-to-date. If the logs end with the same term, then     参数lasti和term分别是Candidate节点的最大记录索引值和最大任期号(即MsgVote
// whichever log has the larger lastIndex is more up-to-date. If the logs are  请求(Candidate发送的选举请求)携带的Index和LogTerm)
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	//比较日志新旧的方式：先比较任期号，任期号相同时再比较索引值
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)			//查询指定索引值对应的Entry记录的Term值
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)			//边界检测，检测的条件是l.firstIndex() <= lo < hi <= l.lastIndex()。
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.unstable.offset {						//如果lo小于unstable.offset(unstable中第一条记录的索引),需要从raftLog.storage中获取记录
		//从Storage中获取记录，可能只能从Storage中取前半部分(也就是到lo~1.unstable.offset部分)；maxSizze会限制获取的记录切片的总字节数
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {					//异常处理
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation	从Storage中取出的记录已经达到了maxSize的上限，则直接返回
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	if hi > l.unstable.offset {						//从unstable中取出后半部分，即l.unstable.offset~hi的日志记录
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {							//将Storage中获取的记录与unstable中获取的记录进行合并
			ents = append([]pb.Entry{}, ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil			//限制获取的记录切片的总字节数
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
