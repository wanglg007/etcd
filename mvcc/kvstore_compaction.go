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
	"encoding/binary"
	"time"
)
//该方法完成了对BoltDB中储存的键值对的压缩，在该方法会通过UnsafeRange()方法批量查询待删除的Key(revision)，然后逐个调用UnsafeDelete()方法进行删除，全部的待压缩
//Key被处理完成之后，会向meta Bucket中写入此次压缩的相关信息，其中Key为"finishedCompactRev"。
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer func() { dbCompactionTotalDurations.Observe(float64(time.Since(totalStart) / time.Millisecond)) }()
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()
	//这里将compactMainRev+1写入end([]byte类型)中(Compact方法已经更新过compactMainRev)，作为范围查询的结束key(revision)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	batchsize := int64(10000)												//范围查询的上限
	last := make([]byte, 8+1+8)												//范围查询的起始key(revision)
	for {
		var rev revision

		start := time.Now()
		tx := s.b.BatchTx()													//获取读写事务
		tx.Lock()															//加锁

		keys, _ := tx.UnsafeRange(keyBucketName, last, end, batchsize)		//调用UnsafeRange()方法对key Bucket进行范围查询
		for _, key := range keys {											//遍历上述查询到的Key(revision)，并逐个调用UnsageDelete()方法进行删除
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(keyBucketName, key)
				keyCompactions++
			}
		}

		if len(keys) < int(batchsize) {										//最后一次批量处理
			//将compactMainRev封装成revision，并写入meta Bucket中，其Key为"finishedCompactRev"
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock()														//读写事务使用完毕，解锁
			plog.Printf("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			return true
		}

		// update last			更新last，下次范围查询的起始key依然是last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		tx.Unlock()
		dbCompactionPauseDurations.Observe(float64(time.Since(start) / time.Millisecond))

		select {				//每次范围处理结束之后
		case <-time.After(100 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
