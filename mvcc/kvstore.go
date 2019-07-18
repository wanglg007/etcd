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

package mvcc

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/schedule"
	"github.com/coreos/pkg/capnslog"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
	ErrCanceled  = errors.New("mvcc: watcher is canceled")
	ErrClosed    = errors.New("mvcc: closed")

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing

// ConsistentIndexGetter is an interface that wraps the Get method.
// Consistent index is the offset of an entry in a consistent replicated log.
type ConsistentIndexGetter interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64
}

type store struct {
	ReadView
	WriteView

	// consistentIndex caches the "consistent_index" key's value. Accessed
	// through atomics so must be 64-bit aligned.
	consistentIndex uint64

	// mu read locks for txns and write locks for non-txn store changes.
	// 在开启只读/读写事务时，需要获取该读锁进行同步，即在Read()方法和Write()方法中获取该读锁，在End()方法中释放。在进行压缩等非事务性的操作时，需要加写锁进行同步
	mu sync.RWMutex

	ig ConsistentIndexGetter

	b       backend.Backend				//当前store实例关联的后端存储
	kvindex index						//当前store实例关联的内存索引

	le lease.Lessor						//租约相关的内容

	// revMuLock protects currentRev and compactMainRev.					在修改currentRev字段和compactMainRev字段时，需要获取该锁进行同步
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.	该字段记录当前的revision信息(main revision部分的值)
	currentRev int64
	// compactMainRev is the main revision of the last compaction.		该字段记录最近一次压缩后最小的revision信息(main revision部分的值)
	compactMainRev int64

	// bytesBuf8 is a byte slice of length 8                            索引缓冲区，主要用于记录ConsistentIndex
	// to avoid a repetitive allocation in saveIndex.
	bytesBuf8 []byte

	fifoSched schedule.Scheduler										//FIFO调度器

	stopc chan struct{}
}
// 该函数完成store实例的初始化过程
// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) *store {
	s := &store{
		b:       b,						//初始化backend字段
		ig:      ig,
		kvindex: newTreeIndex(),		//初始化kvindex字段

		le: le,

		currentRev:     1,				//currentRev字段初始化为1
		compactMainRev: -1,				//compactMainRev字段初始化为-1

		bytesBuf8: make([]byte, 8),
		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),
	}
	s.ReadView = &readView{s}		//创建readView实例和writeView实例
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write() })
	}

	tx := s.b.BatchTx()					//获取backend的读写事务，创建名为"key"和"meta"的两个Bucket，然后提交事务
	tx.Lock()
	tx.UnsafeCreateBucket(keyBucketName)
	tx.UnsafeCreateBucket(metaBucketName)
	tx.Unlock()
	s.b.ForceCommit()

	if err := s.restore(); err != nil {	//从backend中恢复当前store的所有状态，其中包括内存中的BTree索引等
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		s.mu.Lock()
		select {
		case <-s.stopc:
		default:
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
		}
		s.mu.Unlock()
		return
	}
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	start := time.Now()

	s.b.ForceCommit()
	h, err := s.b.Hash(DefaultIgnores)

	hashDurations.Observe(time.Since(start).Seconds())
	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.Lock()
	defer tx.Unlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(keyBucketName)
	err = tx.UnsafeForEach(keyBucketName, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevDurations.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}
//该方法用于实现键值对的压缩。该方法首先将传入的参数转换成revision实例，然后在meta Bucket中记录此次压缩的键值对信息(Key为scheduledCompactRev,Value为revision),
//之后调用treeIndex.Compact()方法完成对内存索引的压缩，最后通过FIFO Scheduler异步完成对BoltDB的压缩操作。
func (s *store) Compact(rev int64) (<-chan struct{}, error) {
	s.mu.Lock()								//获取mu上的写锁和revMu上的写锁
	defer s.mu.Unlock()
	s.revMu.Lock()
	defer s.revMu.Unlock()

	if rev <= s.compactMainRev {
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		return ch, ErrCompacted
	}
	if rev > s.currentRev {					//检测传入的rev参数是否合法
		return nil, ErrFutureRev
	}

	start := time.Now()						//记录当前时间，主要是为了后面记录监控

	s.compactMainRev = rev					//更新compactMainRev

	rbytes := newRevBytes()					//将传入的rev值封装成revision实例，并写入[]byte切片中
	revToBytes(revision{main: rev}, rbytes)

	tx := s.b.BatchTx()						//获取BoltDB中的读写事务，并将上面创建的revision实例写入到名为"meta"的Bucket中
	tx.Lock()
	tx.UnsafePut(metaBucketName, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit()						//提交当前读写事务

	keep := s.kvindex.Compact(rev)			//对内存索引进行压缩。其返回值是此次压缩过程中涉及的revision实例
	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {				//检测ctx
			s.compactBarrier(ctx, ch)
			return
		}
		if !s.scheduleCompaction(rev, keep) {		//这里是真正对BoltDB进行压缩的地方
			s.compactBarrier(nil, ch)
			return
		}
		close(ch)							//当正常压缩结束之后，会关闭该通道，通知其他监听的。
	}
	//store.fifoSched是一个FIFO Scheduler，在Scheduler接口中定义了一个Schedule(j Job)方法，该方法会将Job放入调度器进行调度，其中的Job实际上就是一个
	//func(context.Context)函数。这里将对BoltDB的压缩操作(即上面定义的j函数)放入FIFO调度器中，异步执行。
	s.fifoSched.Schedule(j)

	indexCompactionPauseDurations.Observe(float64(time.Since(start) / time.Millisecond))
	return ch, nil
}

// DefaultIgnores is a map of keys to ignore in hash checking.
var DefaultIgnores map[backend.IgnoreKey]struct{}

func init() {
	DefaultIgnores = map[backend.IgnoreKey]struct{}{
		// consistent index might be changed due to v2 internal sync, which
		// is not controllable by the user.
		{Bucket: string(metaBucketName), Key: string(consistentIndexKeyName)}: {},
	}
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.b.BatchTx()
	tx.Lock()
	s.saveIndex(tx)
	tx.Unlock()
	s.b.ForceCommit()
}
//在介绍Raft协议时，Follower节点在收到快照数据时，会使用快照数据恢复当前节点的状态。该过程会调用store.Restore()方法完成内存索引和store中其他状态的恢复。
func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()											//获取mu上的写锁，关闭当前的FIFO Scheduler
	defer s.mu.Unlock()

	close(s.stopc)
	s.fifoSched.Stop()

	atomic.StoreUint64(&s.consistentIndex, 0)
	s.b = b												//更新store.b字段，指向新的backend实例
	s.kvindex = newTreeIndex()							//更新store.kvindex字段，指向新的内存索引
	s.currentRev = 1									//重置currentRev字段和compactMainRev字段
	s.compactMainRev = -1
	s.fifoSched = schedule.NewFIFOScheduler()			//更新fifoSched字段，指向新的FIFO Scheduler
	s.stopc = make(chan struct{})

	return s.restore()									//调用restore()开始的恢复内存索引
}
//该方法恢复内存索引的大致逻辑为：首先批量读取BoltDB中所有的键值对数据，然后将每个键值对封装成revKeyValue实例，并写入一个通道中，最后由一个单独的goroutine读取
//该通道，并完成内存索引的恢复。该方法通过UnsafeRange()方法从BoltDB中查询到的键值对，然后通过restoreChunk()函数转换成revKeyValue实例，并写入rkvc通道中。
func (s *store) restore() error {
	b := s.b

	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	//创建min和max，后续在BoltDB中进行范围查询时的起始Key和结束Key就是min和max
	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, min)									//min为1_0
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)	//max为MaxInt64_MaxInt64

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	tx := s.b.BatchTx()													//获取读写事务，并加锁
	tx.Lock()
	//调用UnsafeRange()方法，在meta Bucket中查询上次的压缩完成时的相关记录(Key为finishedCompactRev)
	_, finishedCompactBytes := tx.UnsafeRange(metaBucketName, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 {									//根据查询结果，恢复store.compactMainRev字段
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main
		plog.Printf("restore compact to %d", s.compactMainRev)
	}
	//调用UnsafeRange()方法，在meta Bucket中查询上次的压缩启动时的相关记录(Key为scheduledCompactRev)
	_, scheduledCompactBytes := tx.UnsafeRange(metaBucketName, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	if len(scheduledCompactBytes) != 0 {								//根据查询结果，更新scheduledCompact字段
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	//在restoreIntoIndex()方法中会启动一个单独的goroutine,用于接收从backend中读取的键值对数据，并恢复到新建的内存索引中(store.kvindex)。
	rkvc, revc := restoreIntoIndex(s.kvindex)
	for {
		//调用UnsafeRange()方法查询BoltDB中的Key Bucket，返回键值对数量的上限是restoreChunkKeys，默认是10000
		keys, vals := tx.UnsafeRange(keyBucketName, min, max, int64(restoreChunkKeys))				//查询restoreChunkKeys数量的键值对
		if len(keys) == 0 {								//查询结果为空，则直接结束当前for循环
			break
		}
		// rkvc blocks if the total pending keys exceeds the restore	将查询到的键值对数据写入rkvc这个通道中，并由restoreIntoIndex()方法中创建的goroutine
		// chunk size to keep keys from consuming too much memory.      进行处理。
		restoreChunk(rkvc, keys, vals, keyToLease)		//调用restoreChunk()方法
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			break										//范围查询得到的结果数小于restoreChunkKeys，即表示最后一次查询
		}
		// next set begins after where this one ended	更新min作为下一次范围查询的起始key
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	close(rkvc)											//关闭rkvc通道
	s.currentRev = <-revc								//从revc通道中获取恢复之后的currentRev

	// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
	// the correct revision should be set to compaction revision in the case, not the largest revision
	// we have seen.
	if s.currentRev < s.compactMainRev {				//校正currentRev和scheduledCompact
		s.currentRev = s.compactMainRev
	}
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	for key, lid := range keyToLease {								//租期相关的处理
		if s.le == nil {
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})		//绑定键值对与指定的Lease实例
		if err != nil {
			plog.Errorf("unexpected Attach error: %v", err)
		}
	}

	tx.Unlock()											//读写事务结束，释放锁

	if scheduledCompact != 0 {							//如果在开始恢复之前，存在未执行完的压缩操作，则重启该压缩操作
		s.Compact(scheduledCompact)
		plog.Printf("resume scheduled compaction at %d", scheduledCompact)
	}

	return nil
}

type revKeyValue struct {
	key  []byte					//BoltDB中的Key值，可以转换得到的revision实例
	kv   mvccpb.KeyValue		//BoltDB中保存的Value值
	kstr string					//原始的Key值
}
//启动一个后台的goroutine，读取rkvc通道中的revKeyValue实例，并将其中的键值对数据恢复到内存索引中等一系列操作。
func restoreIntoIndex(idx index) (chan<- revKeyValue, <-chan int64) {
	//创建两个通道，其中rkvc通道就是用来传递revKeyValue实例的通道
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {				//启动一个单独的goroutine
		currentRev := int64(1)							//记录当前遇到的最大的main revision值
		defer func() { revc <- currentRev }()			//在该goroutine接收时(即rkvc通道被关闭时)，将当前已知的最大main revision值写入revc通道中，待其他goroutine读取
		// restore the tree index from streaming the unordered index.
		// 虽然BTree的查找效率很高，但是随着BTree的层次的加深，效率也随之下降，这里使用kiCache这个map做了一层缓存，更加高效的查找对应的keyIndex实例。前面从BoltDB中
		// 读取到的键值对数据并不是按照原始Key值进行排序的，如果直接向BTree中写入，则可能会引起节点的分裂等变换操作，效率比较低。所以这里使用kiCache这个map做了一层
		// 缓存。
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		for rkv := range rkvc {								//从rkvc通道中读取revKeyValue实例
			ki, ok := kiCache[rkv.kstr]							//先查询缓存
			// purge kiCache if many keys but still missing in the cache
			if !ok && len(kiCache) >= restoreChunkKeys {		//如果kiCache中缓存了大量的Key，但是依然没有命中，则清理缓存
				i := 10
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {							//只清理10个Key
						break
					}
				}
			}
			// cache miss, fetch from tree index if there		如果缓存未命中，则从内存索引中查找对应的keyIndex实例，并添加到缓存中
			if !ok {
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			rev := bytesToRev(rkv.key)							//将revKeyValue.key转换成revision实例
			currentRev = rev.main								//更新currentRev值
			if ok {
				if isTombstone(rkv.key) {						//当前revKeyValue实例对应一个tombstone键值对
					ki.tombstone(rev.main, rev.sub)				//在对应的keyIndex实例中插入tombstone
					continue
				}
				ki.put(rev.main, rev.sub)						//在对应keyIndex实例中添加正常的revision信息
			} else if !isTombstone(rkv.key) {
				//如果从内存索引中依然未查询到对应的keyIndex实例，则需要填充keyIndex实例中其他字段，并添加到内存索引中
				ki.restore(revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				kiCache[rkv.kstr] = ki							//同时会将该keyIndex实例添加到kiCache缓存中
			}
		}
	}()
	return rkvc, revc
}

func restoreChunk(kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	for i, key := range keys {								//遍历读取到的revision信息
		rkv := revKeyValue{key: key}						//创建revKeyValue实例
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {	//反序列化得到KeyValue
			plog.Fatalf("cannot unmarshal event: %v", err)
		}
		rkv.kstr = string(rkv.kv.Key)						//记录对应的原始Key值
		if isTombstone(key) {								//如果是Tombstone，则从keyToLease中删除
			delete(keyToLease, rkv.kstr)					//删除tombstone标识的键值对
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
			keyToLease[rkv.kstr] = lid						//如果键值对有绑定的Lease实例，则记录到keyToLease中
		} else {
			delete(keyToLease, rkv.kstr)					//如果该键值对没有绑定Lease，则从keyToLease中删除
		}
		kvc <- rkv											//将上述revKeyValue实例写入rkvc通道中，等待处理
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

func (s *store) saveIndex(tx backend.BatchTx) {
	if s.ig == nil {
		return
	}
	bs := s.bytesBuf8
	ci := s.ig.ConsistentIndex()								//将ConsistentIndex值写入bytesBuf8缓冲区中
	binary.BigEndian.PutUint64(bs, ci)
	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs)	//将ConsistentIndex值记录到meta Bucket中
	atomic.StoreUint64(&s.consistentIndex, ci)
}

func (s *store) ConsistentIndex() uint64 {
	if ci := atomic.LoadUint64(&s.consistentIndex); ci > 0 {
		return ci
	}
	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	_, vs := tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}
	v := binary.BigEndian.Uint64(vs[0])
	atomic.StoreUint64(&s.consistentIndex, v)
	return v
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(b []byte) []byte {
	if len(b) != revBytesLen {
		plog.Panicf("cannot append mark to non normal revision bytes")
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
