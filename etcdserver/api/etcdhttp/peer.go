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

package etcdhttp

import (
	"encoding/json"
	"net/http"

	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api"
	"github.com/coreos/etcd/lease/leasehttp"
	"github.com/coreos/etcd/rafthttp"
)

const (
	peerMembersPrefix = "/members"
)

// NewPeerHandler generates an http.Handler to handle etcd peer requests.
func NewPeerHandler(s etcdserver.ServerPeer) http.Handler {
	return newPeerHandler(s.Cluster(), s.RaftHandler(), s.LeaseHandler())
}

func newPeerHandler(cluster api.Cluster, raftHandler http.Handler, leaseHandler http.Handler) http.Handler {
	//创建peerMemberHandler实例，在前面初始化EtcdServer实例时会从远端节点请求当前集群的信息，而远端节点就是通过该Handler对此请求进行响应的。
	mh := &peerMembersHandler{
		cluster: cluster,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)					//使用默认的Handler，直接返回404状态码
	mux.Handle(rafthttp.RaftPrefix, raftHandler)				//注册Transport.Handler()方法返回的Handler
	mux.Handle(rafthttp.RaftPrefix+"/", raftHandler)
	mux.Handle(peerMembersPrefix, mh)							//将上述peerMemberHandler实例注册到"/member"路径上
	if leaseHandler != nil {									//注册lease相关的Handler实例
		mux.Handle(leasehttp.LeasePrefix, leaseHandler)
		mux.Handle(leasehttp.LeaseInternalPrefix, leaseHandler)
	}
	mux.HandleFunc(versionPath, versionHandler(cluster, serveVersion))	//注册versionHandler，用于返回当前节点的版本信息
	return mux
}

type peerMembersHandler struct {
	cluster api.Cluster
}

func (h *peerMembersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "GET") {
		return
	}
	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != peerMembersPrefix {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	ms := h.cluster.Members()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ms); err != nil {
		plog.Warningf("failed to encode members response (%v)", err)
	}
}
