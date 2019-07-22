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
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore					//持久化存储，用于保存用户提交的键值对信息
	//当用户发送POST(或DELETE)请求时，会被认为是发送一个集群节点增加(或删除)的请求，httpKVAPI会将该请求的信息写入confChangeC通道，raftNode实例会读取confChange通道
	//并进行相应处理。
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI						//获取请求的URI作为key
	switch {
	case r.Method == "PUT":					//PUT请求的处理(PUT请求表示向kvstore市里中添加或更新指定的键值对数据)
		v, err := ioutil.ReadAll(r.Body)	//读取HTTP请求体
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		//在kvstore.Propose()方法中会对键值对进行序列化，之后将结果写入proposeC通道，后续raftNode会读取其中数据进行处理
		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)				//向用户返回相应的状态码
	case r.Method == "GET":								//GET请求表示从kvstore实例中读取指定的键值对数据
		if v, ok := h.store.Lookup(key); ok {			//直接从kvstore中读取指定的键值对数据，并返回给用户
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)	//返回404
		}
	case r.Method == "POST":							//POST请求表示向集群中新增指定的节点
		url, err := ioutil.ReadAll(r.Body)				//读取请求头，获取新加节点的URL
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)				//解析URI得到新增节点的id
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{						//创建ConfChange消息
			Type:    raftpb.ConfChangeAddNode,			//ConfChangeAddNode表示新增节点
			NodeID:  nodeId,							//指定新增节点的ID
			Context: url,								//指定新增节点的URL
		}
		h.confChangeC <- cc								//将ConfChange实例写入confChangeC通道中

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)				//返回相应状态码
	case r.Method == "DELETE":							//DELETE请求表示从集群中删除指定的节点
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)				//解析key得到的待删除的节点id
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,			//ConfChangeRemoveNode表示删除指定节点
			NodeID: nodeId,									//指定待删除的节点id
		}
		h.confChangeC <- cc									//将ConfChange实例发送到confChangeC通道中

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)					//向客户端返回状态码为204的HTTP响应
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
// 该方法会启动一个单独的后台goroutine来监听指定的地址，接收用来发来的HTTP请求。
// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{										//创建http.Server用于接收HTTP请求
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{								//设置http.Server的Handler字段
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	go func() {	//启动单独的goroutine来监听Addr指定的地址，当有HTTP请求时，http.Server会创建对应的goroutine，并调用httpKVAPI.ServeHTTP方法进行处理
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
