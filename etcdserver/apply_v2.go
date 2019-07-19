// Copyright 2016 The etcd Authors
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
	"encoding/json"
	"path"
	"time"

	"github.com/coreos/etcd/etcdserver/api"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/store"
	"github.com/coreos/go-semver/semver"
)

// ApplierV2 is the interface for processing V2 raft messages
type ApplierV2 interface {
	Delete(r *RequestV2) Response
	Post(r *RequestV2) Response
	Put(r *RequestV2) Response
	QGet(r *RequestV2) Response
	Sync(r *RequestV2) Response
}

func NewApplierV2(s store.Store, c *membership.RaftCluster) ApplierV2 {
	return &applierV2store{store: s, cluster: c}
}

type applierV2store struct {
	store   store.Store
	cluster *membership.RaftCluster
}

func (a *applierV2store) Delete(r *RequestV2) Response {
	switch {
	case r.PrevIndex > 0 || r.PrevValue != "":
		return toResponse(a.store.CompareAndDelete(r.Path, r.PrevValue, r.PrevIndex))
	default:
		return toResponse(a.store.Delete(r.Path, r.Dir, r.Recursive))
	}
}

func (a *applierV2store) Post(r *RequestV2) Response {
	return toResponse(a.store.Create(r.Path, r.Dir, r.Val, true, r.TTLOptions()))
}

func (a *applierV2store) Put(r *RequestV2) Response {
	ttlOptions := r.TTLOptions()												//根据请求内容创建TTLOptionSet实例，其中会设置节点的超时时间，此次请求是否为刷新操作
	exists, existsSet := pbutil.GetBool(r.PrevExist)							//修改节点之前是否存在
	switch {
	case existsSet:
		if exists {
			if r.PrevIndex == 0 && r.PrevValue == "" {
				//未提供PreIndex和PreValue信息，则直接调用Update()方法更新节点值
				return toResponse(a.store.Update(r.Path, r.Val, ttlOptions))
			}
			//提供PreIndex和PreValue信息，则调用CompareAndSwap方法更新节点值。
			return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
		}
		return toResponse(a.store.Create(r.Path, r.Dir, r.Val, false, ttlOptions))
	case r.PrevIndex > 0 || r.PrevValue != "":
		return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
	default:
		//操作的节点时"/0/members"下的节点集群成员信息节点，注意，不会修改V2存储中对应的节点
		if storeMemberAttributeRegexp.MatchString(r.Path) {
			id := membership.MustParseMemberIDFromKey(path.Dir(r.Path))						//从节点的路径信息中解析得到节点id
			var attr membership.Attributes													//将节点值反序列化
			if err := json.Unmarshal([]byte(r.Val), &attr); err != nil {
				plog.Panicf("unmarshal %s should never fail: %v", r.Val, err)
			}
			if a.cluster != nil {															//更新RaftCluster中对应节点的信息
				a.cluster.UpdateAttributes(id, attr)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}
		if r.Path == membership.StoreClusterVersionKey() {									//操作的节点时"/0/version"
			if a.cluster != nil {															//更新RaftCluster中的版本信息
				a.cluster.SetVersion(semver.Must(semver.NewVersion(r.Val)), api.UpdateCapability)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}
		//如果不是上述两种节点，则直接调用Set()方法更新对应节点值
		return toResponse(a.store.Set(r.Path, r.Dir, r.Val, ttlOptions))
	}
}

func (a *applierV2store) QGet(r *RequestV2) Response {
	return toResponse(a.store.Get(r.Path, r.Recursive, r.Sorted))
}

func (a *applierV2store) Sync(r *RequestV2) Response {
	a.store.DeleteExpiredKeys(time.Unix(0, r.Time))
	return Response{}
}

// applyV2Request interprets r as a call to store.X and returns a Response interpreted
// from store.Event
func (s *EtcdServer) applyV2Request(r *RequestV2) Response {
	defer warnOfExpensiveRequest(time.Now(), r, nil, nil)

	switch r.Method {
	case "POST":
		return s.applyV2.Post(r)
	case "PUT":
		return s.applyV2.Put(r)
	case "DELETE":
		return s.applyV2.Delete(r)
	case "QGET":
		return s.applyV2.QGet(r)
	case "SYNC":
		return s.applyV2.Sync(r)
	default:
		// This should never be reached, but just in case:
		return Response{Err: ErrUnknownMethod}
	}
}

func (r *RequestV2) TTLOptions() store.TTLOptionSet {
	refresh, _ := pbutil.GetBool(r.Refresh)
	ttlOptions := store.TTLOptionSet{Refresh: refresh}
	if r.Expiration != 0 {
		ttlOptions.ExpireTime = time.Unix(0, r.Expiration)
	}
	return ttlOptions
}

func toResponse(ev *store.Event, err error) Response {
	return Response{Event: ev, Err: err}
}
