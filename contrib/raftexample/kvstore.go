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

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raft
type kvstore struct {
	//httpKVAPI处理HTTP PUT请求时，会调用kvstore.Propose()方法将用户请求的数据写入proposeC通道中，之后raftNode会从该通道中读取数据并进行处理。
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	//该字段用来存储键值对的map，其中存储的键值都是String类型
	kvStore     map[string]string // current committed key-value pairs
	//该字段负责读取快照文件
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {								//循环读取commitC通道
		if data == nil {										//读取到nil时，则表示需要读取快照数据
			// done replaying log; new data incoming			通过snapshotter读取快照数据
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {			//加载快照数据
				log.Panic(err)
			}
			continue
		}

		var dataKv kv																//将读取到的数据进行反序列化得到KV实例
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()																	//加锁
		s.kvStore[dataKv.Key] = dataKv.Val											//将读取到的键值对保存到kvStore中
		s.mu.Unlock()																//解锁
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}
//该方法会将快照数据反序列化得到一个map实例，然后直接替换当前的kvStore实例。
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {		//快照数据反序列化
		return err
	}
	s.mu.Lock()														//加锁
	s.kvStore = store												//替换kvStore字段
	s.mu.Unlock()													//解锁
	return nil
}
