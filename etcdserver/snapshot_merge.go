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

package etcdserver

import (
	"io"

	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
)

// createMergedSnapshotMessage creates a snapshot message that contains: raft status (term, conf),		该方法会将V2版本存储和V3版本存储封装成snap.Message实例。
// a snapshot of v2 store inside raft.Snapshot as []byte, a snapshot of v3 KV in the top level message
// as ReadCloser.
func (s *EtcdServer) createMergedSnapshotMessage(m raftpb.Message, snapt, snapi uint64, confState raftpb.ConfState) snap.Message {
	// get a snapshot of v2 store as []byte
	clone := s.store.Clone()						//复制一份V2存储的数据，并转换成JSON格式
	d, err := clone.SaveNoCopy()
	if err != nil {
		plog.Panicf("store save should never fail: %v", err)
	}

	// commit kv to write metadata(for example: consistent index).
	s.KV().Commit()									//提交V3存储中当前的读写事务
	dbsnap := s.be.Snapshot()						//获取V3存储快照，其实就是对BoltDB数据库进行快照
	// get a snapshot of v3 KV as readCloser
	rc := newSnapshotReaderCloser(dbsnap)

	// put the []byte snapshot of store into raft snapshot and return the merged snapshot with
	// KV readCloser snapshot.
	snapshot := raftpb.Snapshot{					//将V2存储的快照数据和相关元数据写入raftpb.Snapshot实例中
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapi,
			Term:      snapt,
			ConfState: confState,
		},
		Data: d,
	}
	m.Snapshot = snapshot							//将raftpb.Snapshot实例封装到MsgSnap消息中
	//将消息MsgSnap消息和V3存储中的数据封装成snap.Message实例返回
	return *snap.NewMessage(m, rc, dbsnap.Size())
}

func newSnapshotReaderCloser(snapshot backend.Snapshot) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		n, err := snapshot.WriteTo(pw)
		if err == nil {
			plog.Infof("wrote database snapshot out [total bytes: %d]", n)
		} else {
			plog.Warningf("failed to write database snapshot out [written bytes: %d]: %v", n, err)
		}
		pw.CloseWithError(err)
		err = snapshot.Close()
		if err != nil {
			plog.Panicf("failed to close database snapshot: %v", err)
		}
	}()
	return pr
}
