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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/coreos/bbolt"
)
//批量读写事务的抽象
type BatchTx interface {
	ReadTx															//内嵌ReadTx接口
	UnsafeCreateBucket(name []byte)									//创建Bucket
	UnsafePut(bucketName []byte, key []byte, value []byte)			//向指定Bucket中添加键值对
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)		//向指定Bucket中添加键值对
	UnsafeDelete(bucketName []byte, key []byte)						//在指定Bucket中删除指定的键值对
	// Commit commits a previous tx and begins a new writable one.
	Commit()														//提交当前的读写事务，之后立即打开一个新的读写事务
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()													//提交当前的读写事务，之后并不会再打开新的读写事务
}

type batchTx struct {
	sync.Mutex
	tx      *bolt.Tx		//该batchTx实例底层封装的bolt.Tx实例，即BoltDB层面的读写事务
	backend *backend		//该batchTx实例关联的backend实例

	pending int				//当前事务中执行的修改操作个数，在当前读写事务提交时，该值会被重置为0
}
//该方法直接调用BoltDB的API创建相应的Bucket实例。
func (t *batchTx) UnsafeCreateBucket(name []byte) {
	_, err := t.tx.CreateBucket(name)
	if err != nil && err != bolt.ErrBucketExists {
		plog.Fatalf("cannot create bucket %s (%v)", name, err)
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTx) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	bucket := t.tx.Bucket(bucketName)							//通过BoltDB提供的API获取指定的Bucket实例
	if bucket == nil {
		plog.Fatalf("bucket %s does not exist", bucketName)
	}
	if seq {													//如果是顺序写入，则将填充率设置成90%
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	if err := bucket.Put(key, value); err != nil {				//调用BoltDB提供的API写入键值对时
		plog.Fatalf("cannot put key into bucket (%v)", err)
	}
	t.pending++													//递增pending
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketName)							//在BoltDB中查询指定的Bucket
	if bucket == nil {
		plog.Fatalf("bucket %s does not exist", bucketName)
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {								//如果没有指定endKey，则直接查找指定key对应的键值对并返回
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}
	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {		//获取指定Bucket实例上的Cursor并从key位置开始遍历
		vs = append(vs, cv)					//记录符合条件的value值
		keys = append(keys, ck)				//记录符合条件的key值
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketName []byte, key []byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		plog.Fatalf("bucket %s does not exist", bucketName)
	}
	err := bucket.Delete(key)
	if err != nil {
		plog.Fatalf("cannot delete key from bucket (%v)", err)
	}
	t.pending++
}
//该方法会遍历指定Bucket的缓存和Bucket中的全部键值对，并通过visitor回调函数处理这些遍历到的键值对。
// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucketName, visitor)			//遍历BoltDB中的键值对
}

func unsafeForEach(tx *bolt.Tx, bucket []byte, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil {						//查找指定的Bucket实例
		return b.ForEach(visitor)								//调用Bucket.ForEach()方法进行遍历
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {			//检测当前事务的修改操作数量是否达到上限
		t.commit(false)						//提交当前读写事务，并开启新的事务
	}
	t.Mutex.Unlock()								//释放锁
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {			//当前读写事务中未进行任何修改操作，则无需开启新事务
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit()					//通过BoltDB提供的API提交当前读写事务
		// gofail: var afterCommit struct{}

		commitDurations.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0							//重置pending字段
		if err != nil {
			plog.Fatalf("cannot commit tx (%s)", err)
		}
	}
	if !stop {
		t.tx = t.backend.begin(true)		//开启新的读写事务
	}
}

type batchTxBuffered struct {
	batchTx
	buf txWriteBuffer
}

func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		batchTx: batchTx{backend: backend},									//创建内嵌的batchTx实例
		buf: txWriteBuffer{													//创建txWriteBuffer缓冲区
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true,
		},
	}
	tx.Commit()																//开启一个读写事务
	return tx
}

func (t *batchTxBuffered) Unlock() {
	if t.pending != 0 {														//检测当前读写事务中是否发生了修改操作
		t.backend.readTx.mu.Lock()
		t.buf.writeback(&t.backend.readTx.buf)								//更新readTx的缓存
		t.backend.readTx.mu.Unlock()
		if t.pending >= t.backend.batchLimit {
			t.commit(false)											//如果当前事务的修改操作数量是否达到上限，则提交当前事务，开启新事务
		}
	}
	t.batchTx.Unlock()														//调用batchTx.Unlock()方法完成解锁
}
//该方法通过调用batchTxBuffered.unsafeCommit(false)方法实现的。它会回滚当前的只读事务，提交当前的读写事务，然后开启新的只读事务和读写事务。
func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBuffered) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.mu.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.mu.Unlock()
}

func (t *batchTxBuffered) unsafeCommit(stop bool) {
	if t.backend.readTx.tx != nil {
		if err := t.backend.readTx.tx.Rollback(); err != nil {
			plog.Fatalf("cannot rollback tx (%s)", err)
		}
		t.backend.readTx.reset()
	}

	t.batchTx.commit(stop)

	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBuffered) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucketName, key, value)			//将键值对写入BoltDB
	t.buf.put(bucketName, key, value)					//将键值对写入缓存
}

func (t *batchTxBuffered) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucketName, key, value)		//将键值对写入BoltDB
	t.buf.putSeq(bucketName, key, value)				//将键值对写入缓存
}
