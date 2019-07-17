// Copyright 2017 The etcd Authors
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
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type storeTxnRead struct {
	s  *store				//该storeTxnrRead实例关联的store实例
	tx backend.ReadTx		//该storeTxnrRead实例关联的ReadTx实例

	firstRev int64			//对应于关联的store实例的firstRev和rev字段
	rev      int64
}

func (s *store) Read() TxnRead {
	s.mu.RLock()									//加读锁
	tx := s.b.ReadTx()								//获取只读事务
	s.revMu.RLock()									//要读取store中的compactMainRev字段和currentRev字段，加读锁同步
	tx.Lock()
	firstRev, rev := s.compactMainRev, s.currentRev	//读取store中的compactMainRev字段和currentRev字段，用于创建后续的storeTxnRead实例
	s.revMu.RUnlock()								//读取完compactMainRev字段和currentRev字段，释放读锁
	//metricsTxnWrite同时实现了TxnRead和TxnWrite接口，并在原有的功能上记录监控信息
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev})
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnRead) Rev() int64      { return tr.rev }

func (tr *storeTxnRead) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(key, end, tr.Rev(), ro)
}

func (tr *storeTxnRead) End() {
	tr.tx.Unlock()
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnRead
	tx backend.BatchTx				//当前storeTxnWrite实例关联的读写事务
	// beginRev is the revision where the txn begins; it will write to the next revision.
	beginRev int64					//记录创建当前storeTxnWrite实例时store.currentRev字段的值
	changes  []mvccpb.KeyValue		//在当前读写事务中发生改动的键值对信息
}
//该方法返回storeTxnWrite实例。
func (s *store) Write() TxnWrite {
	s.mu.RLock()					//加读锁
	tx := s.b.BatchTx()				//获取读写事务
	tx.Lock()						//获取读写事务的锁
	tw := &storeTxnWrite{			//创建storeTxnWrite实例
		storeTxnRead: storeTxnRead{s, tx, 0, 0},			//其中first Rev 初始化为0
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

func (tw *storeTxnWrite) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	//如果当前读写事务中有更新键值对的操作，则该方法也需要能查询到这些更新的键值对，所以传入的rev参数是beginRev + 1
	if len(tw.changes) > 0 {
		rev++
	}
	return tw.rangeKeys(key, end, rev, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		return n, int64(tw.beginRev + 1)				//根据当前事务是否有更新操作，决定返回的main revision值
	}
	return 0, int64(tw.beginRev)
}

func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return int64(tw.beginRev + 1)
}
//该方法完成三个操作：一是递增store.currentRev；二是将ConsistentIndex记录到名为"meta"的Bucket中；三是调用Unlock()方法提交当前事务并开启新的读写事务；
func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 {					//检测当前读写事务中是否有修改操作
		//将ConsistentIndex(实际就是当前处理的最后一条Entry记录的索引值)添加记录到BoltDB中名为“meta”的Bucket中
		tw.s.saveIndex(tw.tx)
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock()						//修改store中的currentRev字段，加读锁同步
		tw.s.currentRev++						//递增currentRev
	}
	//签名介绍过batchTxBuffered及batchTx中的Unlock()方法实现，其中会将当前读写事务提交，同事开启新的读写事务
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()						//修改currentRev完成，释放读锁
	}
	tw.s.mu.RUnlock()							//释放读锁
}
//该方法流程为：首先扫描内存索引(treeIndex)，得到对应的revision，然后通过revision查询BoltDB得到真正的键值对数据，最后将键值对数据反序列化成KeyValue并封装成
//RangeResult实例返回。
func (tr *storeTxnRead) rangeKeys(key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	//检测RangeOptions中指定的revision信息是否合法，如果不合法，则根据curRev(对storeTxnRead来说，就是rev字段)当前的compactMainRev值对rev进行校正
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	//调用index.Revisions方法查询指定范围的键值对信息
	revpairs := tr.s.kvindex.Revisions(key, end, int64(rev))
	if len(revpairs) == 0 {					//根据RangeOptions中指定的参数，创建RangeResult实例作为返回值
		return &RangeResult{KVs: nil, Count: 0, Rev: curRev}, nil
	}
	if ro.Count {							//只获取键值对的个数，不需要返回具体的键值对数据，所以只查询索引即可
		return &RangeResult{KVs: nil, Count: len(revpairs), Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes()											//新建一个[]byte切片
	for i, revpair := range revpairs[:len(kvs)] {						//遍历查询到的revision信息
		//将main revision部分和sub revision部分写入start中，两者通过下划线分割
		revToBytes(revpair, revBytes)									//将revision拆分成两个key
		_, vs := tr.tx.UnsafeRange(keyBucketName, revBytes, nil, 0)		//根据revision实例拆分后的结果，对BoltDB进行查询
		if len(vs) != 1 {												//虽然是以了UnsafeRange()方法进行查询，但是其查询结果必然只有一个
			plog.Fatalf("range cannot find rev (%d,%d)", revpair.main, revpair.sub)
		}
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			plog.Fatalf("cannot unmarshal event: %v", err)
		}
	}
	//将kvs封装成RangeResult实例并返回
	return &RangeResult{KVs: kvs, Count: len(revpairs), Rev: curRev}, nil
}
//该方法实现了向存储中追加一个键值对数据的功能
func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1								//当前事务产生的修改对应的main revision部分都是该值
	c := rev
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	_, created, ver, err := tw.s.kvindex.Get(key, rev)	//在内存索引中查找对应的键值对信息，在后面创建KeyValue实例时会使用到这些值
	if err == nil {
		c = created.main
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
	}

	ibytes := newRevBytes()
	//创建此次Put操作对应的revision实例，其中main revision部分为beginRev + 1
	idxRev := revision{main: rev, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes)							//将main revision和sub revision两部分写入ibytes中，后续会作为写入BoltDB的Key

	ver = ver + 1
	kv := mvccpb.KeyValue{								//创建KeyValue实例
		Key:            key,							//原始Key值
		Value:          value,							//原始Value值
		CreateRevision: c,								//如果内存索引中已经存在该键值对，则创建版本不变，否则为新建Key，则将CreationRevision设置成beginRev + 1
		ModRevision:    rev,							//beginRev + 1
		Version:        ver,							//递增version
		Lease:          int64(leaseID),
	}

	d, err := kv.Marshal()								//将上面创建的KeyValue实例序列化
	if err != nil {
		plog.Fatalf("cannot marshal event: %v", err)
	}
	//将Key(main revision和sub revision组成)和Value(上述KeyValue实例序列化的结果)写入BoltDB中名为"key"的Bucket
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	tw.s.kvindex.Put(key, idxRev)						//将原始Key与revision实例的对应关系写入内存索引中
	tw.changes = append(tw.changes, kv)					//将上述KeyValue写入到changes中

	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			plog.Errorf("unexpected error from lease detach: %v", err)
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
}
//该方法首先从内存索引查询待删除的Key和对应的revision实例，然后会调用storeTxnWrite.delete()方法逐个删除。
func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {						//根据当前事务是否有更新操作，决定后续使用的main revision值
		rrev += 1
	}
	keys, revs := tw.s.kvindex.Range(key, end, rrev)//在内存索引中查询待删除的key
	if len(keys) == 0 {
		return 0
	}
	for i, key := range keys {						//遍历待删除的key，并调用delete()方法完成删除
		tw.delete(key, revs[i])
	}
	return int64(len(keys))						//返回删除的key值
}

func (tw *storeTxnWrite) delete(key []byte, rev revision) {
	ibytes := newRevBytes()													//将revision转换成BoltDB中的key
	idxRev := revision{main: tw.beginRev + 1, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes)
	ibytes = appendMarkTombstone(ibytes)									//追加一个't'标识Tombstone

	kv := mvccpb.KeyValue{Key: key}											//创建KeyValue实例，其中只包含key，并进行序列化

	d, err := kv.Marshal()
	if err != nil {
		plog.Fatalf("cannot marshal event: %v", err)
	}
	//将上面生成的key(ibytes)和value(KeyValue序列化后的数据)写入名为"key"的Bucket中
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	err = tw.s.kvindex.Tombstone(key, idxRev)								//在内存索引中添加Tombstone
	if err != nil {
		plog.Fatalf("cannot tombstone an existing key (%s): %v", string(key), err)
	}
	tw.changes = append(tw.changes, kv)										//向changes中追加上述KeyValue实例

	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			plog.Errorf("cannot detach %v", err)
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }
