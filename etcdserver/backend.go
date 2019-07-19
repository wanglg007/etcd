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

package etcdserver

import (
	"fmt"
	"os"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
)

func newBackend(cfg ServerConfig) backend.Backend {
	bcfg := backend.DefaultBackendConfig()
	bcfg.Path = cfg.backendPath()
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != DefaultQuotaBytes {
		// permit 10% excess over quota for disarm
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}
	return backend.New(bcfg)
}

// openSnapshotBackend renames a snapshot db to the current etcd db and opens it.		重建新的Backend实例的函数
func openSnapshotBackend(cfg ServerConfig, ss *snap.Snapshotter, snapshot raftpb.Snapshot) (backend.Backend, error) {
	//根据快照元数据查找对应的BoltDB数据库文件
	snapPath, err := ss.DBFilePath(snapshot.Metadata.Index)
	if err != nil {
		return nil, fmt.Errorf("database snapshot file path error: %v", err)
	}
	if err := os.Rename(snapPath, cfg.backendPath()); err != nil {						//将可用的BoltDB数据库文件移动到指定的目录中
		return nil, fmt.Errorf("rename snapshot file error: %v", err)
	}
	return openBackend(cfg), nil														//新建Backend实例
}
// 该函数会启动一个后台goroutine完成Backend实例的初始化。
// openBackend returns a backend using the current etcd db.
func openBackend(cfg ServerConfig) backend.Backend {
	fn := cfg.backendPath()
	beOpened := make(chan backend.Backend)
	go func() {							//启动一个后台goroutine并将新建的Backend实例写入beOpened通道中
		beOpened <- newBackend(cfg)			//新建Backend实例
	}()
	select {
	case be := <-beOpened:
		return be
	case <-time.After(10 * time.Second):	//初始化Backend实例超时，会输出日志
		plog.Warningf("another etcd process is using %q and holds the file lock, or loading backend file is taking >10 seconds", fn)
		plog.Warningf("waiting for it to exit before starting...")
	}
	return <-beOpened						//阻塞等待Backend实例初始化完成
}

// recoverBackendSnapshot recovers the DB from a snapshot in case etcd crashes		该函数会检测前面创建的Backend是否可用(即包含快照数据所包含的全部Entry记录)，如果
// before updating the backend db after persisting raft snapshot to disk,			可用则继续使用该Backend实例，如果不可用则根据快照的元数据查找可用的BoltDB数据库
// violating the invariant snapshot.Metadata.Index < db.consistentIndex. In this    文件，并创建新的Backend实例。
// case, replace the db with the snapshot db sent by the leader.
func recoverSnapshotBackend(cfg ServerConfig, oldbe backend.Backend, snapshot raftpb.Snapshot) (backend.Backend, error) {
	var cIndex consistentIndex
	kv := mvcc.New(oldbe, &lease.FakeLessor{}, &cIndex)
	defer kv.Close()
	if snapshot.Metadata.Index <= kv.ConsistentIndex() {
		return oldbe, nil					//若当前使用的Backend实例可用，则直接返回
	}
	oldbe.Close()							//旧Backend实例不可用，则关闭旧的Backend实例，并重创建新的Backend实例
	return openSnapshotBackend(cfg, snap.New(cfg.SnapDir()), snapshot)
}
