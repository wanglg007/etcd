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

import pb "github.com/coreos/etcd/etcdserver/etcdserverpb"

const (
	// DefaultQuotaBytes is the number of bytes the backend Size may
	// consume before exceeding the space quota.
	DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB
	// MaxQuotaBytes is the maximum number of bytes suggested for a backend
	// quota. A larger quota may lead to degraded performance.
	MaxQuotaBytes = int64(8 * 1024 * 1024 * 1024) // 8GB
)

// Quota represents an arbitrary quota against arbitrary requests. Each request
// costs some charge; if there is not enough remaining charge, then there are
// too few resources available within the quota to apply the request.
type Quota interface {						//该接口定义了实现限流功能的核心
	// Available judges whether the given request fits within the quota.		检测此次请求是否能通过限流，即当前请求执行后，未达到系统负载的上限，也就未触发限流
	Available(req interface{}) bool
	// Cost computes the charge against the quota for a given request.			计算此次请求所产生的负载
	Cost(req interface{}) int
	// Remaining is the amount of charge left for the quota.					当前系统所能支持的剩余负载量
	Remaining() int64
}

type passthroughQuota struct{}

func (*passthroughQuota) Available(interface{}) bool { return true }
func (*passthroughQuota) Cost(interface{}) int       { return 0 }
func (*passthroughQuota) Remaining() int64           { return 1 }
//该结构体用于限制底层BoltDB中的数据量
type backendQuota struct {
	s               *EtcdServer			//封装了当前节点的EtcdServer实例
	maxBackendBytes int64				//BoltDB数据量的上限值
}

const (
	// leaseOverhead is an estimate for the cost of storing a lease
	leaseOverhead = 64
	// kvOverhead is an estimate for the cost of storing a key's metadata
	kvOverhead = 256
)

func NewBackendQuota(s *EtcdServer) Quota {
	quotaBackendBytes.Set(float64(s.Cfg.QuotaBackendBytes))

	if s.Cfg.QuotaBackendBytes < 0 {
		// disable quotas if negative				创建passthroughQuota实例，它也是Quota接口的实现之一，但它没有实现限流的功能，其Available()方法始终返回true，
		plog.Warningf("disabling backend quota")	//Cost()方法始终返回false
		return &passthroughQuota{}
	}

	if s.Cfg.QuotaBackendBytes == 0 {
		// use default size if no quota size given
		quotaBackendBytes.Set(float64(DefaultQuotaBytes))
		return &backendQuota{s, DefaultQuotaBytes}		//创建backendQuota实例
	}

	if s.Cfg.QuotaBackendBytes > MaxQuotaBytes {
		plog.Warningf("backend quota %v exceeds maximum recommended quota %v", s.Cfg.QuotaBackendBytes, MaxQuotaBytes)
	}
	return &backendQuota{s, s.Cfg.QuotaBackendBytes}		//创建backendQuota实例
}
//该方法会将当前BoltDB中的数据量、此次请求的数据量之和与上限阈值进行比较，从而决定此次请求是否触发限流。
func (b *backendQuota) Available(v interface{}) bool {
	// TODO: maybe optimize backend.Size()
	return b.s.Backend().Size()+int64(b.Cost(v)) < b.maxBackendBytes
}
//请求的数据量是通过该函数得到的。
func (b *backendQuota) Cost(v interface{}) int {
	switch r := v.(type) {
	case *pb.PutRequest:
		return costPut(r)
	case *pb.TxnRequest:
		return costTxn(r)
	case *pb.LeaseGrantRequest:
		return leaseOverhead
	default:
		panic("unexpected cost")
	}
}
//该方法会计算请求的Key值、Value值和相关元数据的字节数之和。
func costPut(r *pb.PutRequest) int { return kvOverhead + len(r.Key) + len(r.Value) }

func costTxnReq(u *pb.RequestOp) int {
	r := u.GetRequestPut()
	if r == nil {
		return 0
	}
	return costPut(r)
}

func costTxn(r *pb.TxnRequest) int {
	sizeSuccess := 0
	for _, u := range r.Success {
		sizeSuccess += costTxnReq(u)
	}
	sizeFailure := 0
	for _, u := range r.Failure {
		sizeFailure += costTxnReq(u)
	}
	if sizeFailure > sizeSuccess {			//与计算r.Success同理，计算r.Failure的数据量
		return sizeFailure
	}
	return sizeSuccess
}

func (b *backendQuota) Remaining() int64 {
	return b.maxBackendBytes - b.s.Backend().Size()
}
