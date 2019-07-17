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
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type RangeOptions struct {
	Limit int64			//此次查询返回的键值对个数的上限
	Rev   int64			//扫描内存索引时使用到的main revision部分的值
	Count bool			//如果将该值设置为true，则只返回键值对个数，并不返回具体的键值对数据
}

type RangeResult struct {
	KVs   []mvccpb.KeyValue		//此次查询得到的键值对数据集合
	Rev   int64
	Count int					//此次查询返回的键值对个数
}
//该接口定义了只读事务相关的视图方法。
type ReadView interface {
	// FirstRev returns the first KV revision at the time of opening the txn.	FirstRev()方法会返回开启当前只读事务时的revision信息，这与Rev()方法相同，但是当
	// After a compaction, the first revision increases to the compaction       进行一次压缩操作之后，该方法的返回值会被更新成压缩时的revision信息，也就是压缩
	// revision.                                                                后的最小revision
	FirstRev() int64

	// Rev returns the revision of the KV at the time of opening the txn.		Rev()方法会返回开启当前只读事务的revision信息
	Rev() int64

	// Range gets the keys in the range at rangeRev.
	// The returned rev is the current revision of the KV when the operation is executed.
	// If rangeRev <=0, range gets the keys at currentRev.
	// If `end` is nil, the request returns the key.
	// If `end` is not nil and not empty, it gets the keys in range [key, range_end).
	// If `end` is not nil and empty, it gets the keys greater than or equal to key.
	// Limit limits the number of keys returned.                               	范围查询
	// If the required rev is compacted, ErrCompacted will be returned.
	Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error)
}
// 该接口表示一个只读事务，其中内嵌ReadView接口，并在其基础上扩展了一个End()方法，该End()方法用来标识当前事务已经完成，并准备提交。
// TxnRead represents a read-only transaction with operations that will not
// block other read transactions.
type TxnRead interface {
	ReadView
	// End marks the transaction is complete and ready to commit.
	End()
}
//该接口定义了读写事务相关的方法。
type WriteView interface {
	// DeleteRange deletes the given range from the store.
	// A deleteRange increases the rev of the store if any key in the range exists.
	// The number of key deleted will be returned.
	// The returned rev is the current revision of the KV when the operation is executed.
	// It also generates one event for each key delete in the event history.
	// if the `end` is nil, deleteRange deletes the key.
	// if the `end` is not nil, deleteRange deletes the keys in range [key, range_end).
	DeleteRange(key, end []byte) (n, rev int64)										//范围删除

	// Put puts the given key, value into the store. Put also takes additional argument lease to
	// attach a lease to a key-value pair as meta-data. KV implementation does not validate the lease
	// id.
	// A put also increases the rev of the store, and generates one event in the event history.
	// The returned rev is the current revision of the KV when the operation is executed.
	Put(key, value []byte, lease lease.LeaseID) (rev int64)							//添加指定的键值对
}
// 该接口表示一个读写事务，其中内嵌了TxnRead接口和WriteView接口，并在两者的基础上扩展一个Changes()方法，该方法会返回自事务开启之后修改的键值对信息。
// TxnWrite represents a transaction that can modify the store.
type TxnWrite interface {
	TxnRead
	WriteView
	// Changes gets the changes made since opening the write txn.
	Changes() []mvccpb.KeyValue
}

// txnReadWrite coerces a read txn to a write, panicking on any write operation.
type txnReadWrite struct{ TxnRead }

func (trw *txnReadWrite) DeleteRange(key, end []byte) (n, rev int64) { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	panic("unexpected Put")
}
func (trw *txnReadWrite) Changes() []mvccpb.KeyValue { return nil }

func NewReadOnlyTxnWrite(txn TxnRead) TxnWrite { return &txnReadWrite{txn} }

type KV interface {
	ReadView									//内嵌了ReadView接口和WriteView接口
	WriteView

	// Read creates a read transaction.			创建只读事务
	Read() TxnRead

	// Write creates a write transaction.		创建读写事务
	Write() TxnWrite

	// Hash computes the hash of the KV's backend.
	Hash() (hash uint32, revision int64, err error)

	// HashByRev computes the hash of all MVCC revisions up to a given revision.
	HashByRev(rev int64) (hash uint32, revision int64, compactRev int64, err error)

	// Compact frees all superseded keys with revisions less than rev.			对整个KV存储进行压缩
	Compact(rev int64) (<-chan struct{}, error)

	// Commit commits outstanding txns into the underlying backend.				提交事务
	Commit()

	// Restore restores the KV store from a backend.							从BoltDB中恢复内存索引
	Restore(b backend.Backend) error
	Close() error
}

// WatchableKV is a KV that can be watched.
type WatchableKV interface {
	KV
	Watchable
}

// Watchable is the interface that wraps the NewWatchStream function.
type Watchable interface {
	// NewWatchStream returns a WatchStream that can be used to
	// watch events happened or happening on the KV.
	NewWatchStream() WatchStream
}

// ConsistentWatchableKV is a WatchableKV that understands the consistency
// algorithm and consistent index.
// If the consistent index of executing entry is not larger than the
// consistent index of ConsistentWatchableKV, all operations in
// this entry are skipped and return empty response.
type ConsistentWatchableKV interface {
	WatchableKV
	// ConsistentIndex returns the current consistent index of the KV.
	ConsistentIndex() uint64
}
