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

package backend

import (
	"bytes"
	"math"
	"sync"

	bolt "github.com/coreos/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")
//只读事务的抽象
type ReadTx interface {
	Lock()
	Unlock()

	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)		//在指定的Bucket中进行范围查找
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error								//遍历指定Bucket中的全部键值对
}

type readTx struct {
	// mu protects accesses to the txReadBuffer			在读写buf中的缓存区数据时，需要获取该锁进行同步
	mu  sync.RWMutex
	//该buffer主要用来缓存Bucket与其中键值对集合的映射关系
	buf txReadBuffer

	// txmu protects accesses to buckets and tx on Range requests.
	txmu    sync.RWMutex								//在进行查询之前，需要获取该锁进行同步
	tx      *bolt.Tx									//该readTx实例底层封装的bolt.Tx实例，即BoltDB层面的只读事务
	buckets map[string]*bolt.Bucket
}

func (rt *readTx) Lock()   { rt.mu.RLock() }
func (rt *readTx) Unlock() { rt.mu.RUnlock() }
//该方法是进行范围查询
func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {													//对非法的limit值进行重新设置
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {		//只有查询safeRangeBucket(即名称为key的Bucket)时，才是真正的范围查询，否则只能返回一个键值对
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)		//首先从缓存中查询键值对
	if int64(len(keys)) == limit {		//检测缓存返回的键值对数量是否达到limit的限制，如果达到limit指定的上限，则直接返回缓存的查询结果
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txmu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txmu.RUnlock()
	if !ok {
		rt.txmu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txmu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return keys, vals
	}
	rt.txmu.Lock()					//获取txmu锁
	c := bucket.Cursor()
	rt.txmu.Unlock()				//是否txmu锁

	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))		//通过unsafeRange()函数从BoltDB中查询
	return append(k2, keys...), append(v2, vals...)					//将查询缓存的结果与查询BoltDB的结果合并，然后返回
}

func (rt *readTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	rt.txmu.Lock()
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txmu.Unlock()
	if err != nil {
		return err
	}
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
}
