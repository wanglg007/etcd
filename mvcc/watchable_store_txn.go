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
	"github.com/coreos/etcd/mvcc/mvccpb"
)
//若当前读写事务中存在更新操作，则会在该方法中触发相应的watcher实例。
func (tw *watchableStoreTxnWrite) End() {
	changes := tw.Changes()				//当前读写事务中执行的更新操作
	if len(changes) == 0 {				//如果当前事务中没有任何更新操作，则直接结束当前事务即可
		tw.TxnWrite.End()
		return
	}

	rev := tw.Rev() + 1
	evs := make([]mvccpb.Event, len(changes))
	for i, change := range changes {	//将当前事务中的更新操作转换成对应的Event事件
		evs[i].Kv = &changes[i]
		if change.CreateRevision == 0 {
			evs[i].Type = mvccpb.DELETE
			evs[i].Kv.ModRevision = rev
		} else {
			evs[i].Type = mvccpb.PUT
		}
	}

	// end write txn under watchable store lock so the updates are visible
	// when asynchronous event posting checks the current store revision
	tw.s.mu.Lock()						//加锁、解锁
	tw.s.notify(rev, evs)				//调用watchableStore.notify()方法，将上述Event事件发送出去
	tw.TxnWrite.End()					//结束当前读写事务
	tw.s.mu.Unlock()
}

type watchableStoreTxnWrite struct {
	TxnWrite
	s *watchableStore
}

func (s *watchableStore) Write() TxnWrite { return &watchableStoreTxnWrite{s.store.Write(), s} }
