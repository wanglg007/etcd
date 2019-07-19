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

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.		Save()方法负责将Eentry记录和HardState状态信息保存到底层的持久化存储上，该方法
	// Save MUST block until st and ents are on stable storage.					可能会阻塞。Storage接口的实现是通过WAL模块将上述数据持久化到WAL日志文件中的。
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.		SaveSnap()方法负责将快照数据持久化到底层的持久化存储上。
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

type storage struct {
	*wal.WAL
	*snap.Snapshotter
}

func NewStorage(w *wal.WAL, s *snap.Snapshotter) Storage {
	return &storage{w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
	walsnap := walpb.Snapshot{					//根据快照的元数据创建对应的walpb.Snapshot实例
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)			//将walpb.Snapshot实例封装成Record记录写入WAL日志文件中
	if err != nil {
		return err
	}
	err = st.Snapshotter.SaveSnap(snap)			//通过Snapshotter将快照数据写入到磁盘
	if err != nil {
		return err
	}
	//根据WAL日志文件的名称及快照的元数据，释放快照之前的WAL日志文件句柄
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}

func readWAL(waldir string, snap walpb.Snapshot) (w *wal.WAL, id, cid types.ID, st raftpb.HardState, ents []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
		if w, err = wal.Open(waldir, snap); err != nil {
			plog.Fatalf("open wal error: %v", err)
		}
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				plog.Fatalf("read wal error (%v) and cannot be repaired", err)
			}
			if !wal.Repair(waldir) {
				plog.Fatalf("WAL error (%v) cannot be repaired", err)
			} else {
				plog.Infof("repaired WAL error (%v)", err)
				repaired = true
			}
			continue
		}
		break
	}
	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	id = types.ID(metadata.NodeID)
	cid = types.ID(metadata.ClusterID)
	return w, id, cid, st, ents
}
