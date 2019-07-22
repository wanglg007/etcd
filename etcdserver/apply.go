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
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/coreos/etcd/auth"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/types"

	"github.com/gogo/protobuf/proto"
)

const (
	warnApplyDuration = 100 * time.Millisecond
)

type applyResult struct {
	resp proto.Message
	err  error
	// physc signals the physical effect of the request has completed in addition
	// to being logically reflected by the node. Currently only used for
	// Compaction requests.
	physc <-chan struct{}
}

// applierV3 is the interface for processing V3 raft messages
type applierV3 interface {
	Apply(r *pb.InternalRaftRequest) *applyResult													//根据InternalRaftRequest的类型调用下面不同的方法进行处理

	Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, error)
	Range(txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error)
	DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error)
	Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, error)

	LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	Alarm(*pb.AlarmRequest) (*pb.AlarmResponse, error)

	Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error)

	AuthEnable() (*pb.AuthEnableResponse, error)
	AuthDisable() (*pb.AuthDisableResponse, error)

	UserAdd(ua *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ua *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ua *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ua *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ua *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ua *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ua *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ua *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ua *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ua *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ua *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ua *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ua *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

type checkReqFunc func(mvcc.ReadView, *pb.RequestOp) error

type applierV3backend struct {
	s *EtcdServer

	checkPut   checkReqFunc
	checkRange checkReqFunc
}

func (s *EtcdServer) newApplierV3Backend() applierV3 {
	base := &applierV3backend{s: s}
	base.checkPut = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestPut(rv, req)
	}
	base.checkRange = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestRange(rv, req)
	}
	return base
}

func (s *EtcdServer) newApplierV3() applierV3 {
	return newAuthApplierV3(
		s.AuthStore(),
		newQuotaApplierV3(s, s.newApplierV3Backend()),
		s.lessor,
	)
}
//该方法会根据请求的类型进行分类处理。
func (a *applierV3backend) Apply(r *pb.InternalRaftRequest) *applyResult {
	ar := &applyResult{}
	defer func(start time.Time) {
		warnOfExpensiveRequest(start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
	}(time.Now())

	// call into a.s.applyV3.F instead of a.F so upper appliers can check individual calls
	// 需要注意，这里调用的是EtcdServer.applyV3对应的方法，而不是直接调用applierV3backend实例的对应方法。在EtcdServer中，APPlyV3Base字段指向了applierV3backend
	// 实例，而applyV3字段则是在applyV3Base基础之上的扩展。
	switch {
	case r.Range != nil:
		ar.resp, ar.err = a.s.applyV3.Range(nil, r.Range)
	case r.Put != nil:
		ar.resp, ar.err = a.s.applyV3.Put(nil, r.Put)
	case r.DeleteRange != nil:
		ar.resp, ar.err = a.s.applyV3.DeleteRange(nil, r.DeleteRange)
	case r.Txn != nil:
		ar.resp, ar.err = a.s.applyV3.Txn(r.Txn)
	case r.Compaction != nil:
		ar.resp, ar.physc, ar.err = a.s.applyV3.Compaction(r.Compaction)
	case r.LeaseGrant != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseGrant(r.LeaseGrant)
	case r.LeaseRevoke != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseRevoke(r.LeaseRevoke)
	case r.Alarm != nil:
		ar.resp, ar.err = a.s.applyV3.Alarm(r.Alarm)
	case r.Authenticate != nil:
		ar.resp, ar.err = a.s.applyV3.Authenticate(r.Authenticate)
	case r.AuthEnable != nil:
		ar.resp, ar.err = a.s.applyV3.AuthEnable()
	case r.AuthDisable != nil:
		ar.resp, ar.err = a.s.applyV3.AuthDisable()
	case r.AuthUserAdd != nil:
		ar.resp, ar.err = a.s.applyV3.UserAdd(r.AuthUserAdd)
	case r.AuthUserDelete != nil:
		ar.resp, ar.err = a.s.applyV3.UserDelete(r.AuthUserDelete)
	case r.AuthUserChangePassword != nil:
		ar.resp, ar.err = a.s.applyV3.UserChangePassword(r.AuthUserChangePassword)
	case r.AuthUserGrantRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserGrantRole(r.AuthUserGrantRole)
	case r.AuthUserGet != nil:
		ar.resp, ar.err = a.s.applyV3.UserGet(r.AuthUserGet)
	case r.AuthUserRevokeRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserRevokeRole(r.AuthUserRevokeRole)
	case r.AuthRoleAdd != nil:
		ar.resp, ar.err = a.s.applyV3.RoleAdd(r.AuthRoleAdd)
	case r.AuthRoleGrantPermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGrantPermission(r.AuthRoleGrantPermission)
	case r.AuthRoleGet != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGet(r.AuthRoleGet)
	case r.AuthRoleRevokePermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleRevokePermission(r.AuthRoleRevokePermission)
	case r.AuthRoleDelete != nil:
		ar.resp, ar.err = a.s.applyV3.RoleDelete(r.AuthRoleDelete)
	case r.AuthUserList != nil:
		ar.resp, ar.err = a.s.applyV3.UserList(r.AuthUserList)
	case r.AuthRoleList != nil:
		ar.resp, ar.err = a.s.applyV3.RoleList(r.AuthRoleList)
	default:
		panic("not implemented")
	}
	return ar
}

func (a *applierV3backend) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (resp *pb.PutResponse, err error) {
	resp = &pb.PutResponse{}							//该方法的最终返回值
	resp.Header = &pb.ResponseHeader{}

	val, leaseID := p.Value, lease.LeaseID(p.Lease)
	if txn == nil {										//如果传入的txn事务为空，则开启新事务
		if leaseID != lease.NoLease {
			//通过lessor.Lookup()方法查找PutRequest携带的LeaseID是否存在，如果不存在，则返回错误信息
			if l := a.s.lessor.Lookup(leaseID); l == nil {
				return nil, lease.ErrLeaseNotFound
			}
		}
		txn = a.s.KV().Write()
		defer txn.End()									//方法结束时关闭该方法中开启的事务
	}

	var rr *mvcc.RangeResult
	if p.IgnoreValue || p.IgnoreLease || p.PrevKv {		//如果设置了IgnoreValue、IgnoreLease或是PrevKv，则需要当前的键值对信息
		rr, err = txn.Range(p.Key, nil, mvcc.RangeOptions{})
		if err != nil {
			return nil, err
		}
	}
	if p.IgnoreValue || p.IgnoreLease {					//如果设置了IgnoreValue或IgnoreLease，则检测当前键值对是否存在，如果不存在则返回错误
		if rr == nil || len(rr.KVs) == 0 {
			// ignore_{lease,value} flag expects previous key-value pair
			return nil, ErrKeyNotFound
		}
	}
	if p.IgnoreValue {									//如果设置了IgnoreValue，则使用当前的Value值
		val = rr.KVs[0].Value
	}
	if p.IgnoreLease {									//同理，如果设置了IgnoreLease，则不会更新键值对绑定的Lease
		leaseID = lease.LeaseID(rr.KVs[0].Lease)
	}
	if p.PrevKv {										//如果设置了PrevKv，则在PutResponse中返回更新前的键值对信息
		if rr != nil && len(rr.KVs) != 0 {
			resp.PrevKv = &rr.KVs[0]
		}
	}
	//调用Put方法完成更新操作，在PutResponse.Header中记录revision信息
	resp.Header.Revision = txn.Put(p.Key, val, leaseID)
	return resp, nil
}
//该方法会根据DeleteRangeRequest中的添加删除指定的键值对信息
func (a *applierV3backend) DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp := &pb.DeleteRangeResponse{}			//返回值
	resp.Header = &pb.ResponseHeader{}
	end := mkGteRange(dr.RangeEnd)

	if txn == nil {								//如果当前事务为空，则开启新的只读事务，并在该Range查询结束后关闭此事务
		txn = a.s.kv.Write()
		defer txn.End()
	}

	if dr.PrevKv {								//如果PrevKv设置为true，则先查询指定范围的键值对，并封装到返回值中
		rr, err := txn.Range(dr.Key, end, mvcc.RangeOptions{})
		if err != nil {
			return nil, err
		}
		if rr != nil {
			resp.PrevKvs = make([]*mvccpb.KeyValue, len(rr.KVs))
			for i := range rr.KVs {
				resp.PrevKvs[i] = &rr.KVs[i]
			}
		}
	}
	//调用DeleteRange()方法删除指定范围的键值对
	resp.Deleted, resp.Header.Revision = txn.DeleteRange(dr.Key, end)
	return resp, nil
}

func (a *applierV3backend) Range(txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp := &pb.RangeResponse{}					//该方法的返回值
	resp.Header = &pb.ResponseHeader{}

	if txn == nil {								//如果只读事务为空，则开启新的只读事务，并在Range查询结束后关闭此事务
		txn = a.s.kv.Read()
		defer txn.End()
	}

	limit := r.Limit
	if r.SortOrder != pb.RangeRequest_NONE ||
		r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0 {
		// fetch everything; sort and truncate afterwards
		limit = 0				//如果指定上述查询条件，则先查询全部符合条件的KV，然后进行limit的过滤
	}
	if limit > 0 {				//修正当前的limit值
		// fetch one extra for 'more' flag
		limit = limit + 1
	}

	ro := mvcc.RangeOptions{	//创建RangeOptions,其中封装了limit、revision等查询条件
		Limit: limit,
		Rev:   r.Revision,
		Count: r.CountOnly,
	}
	//调用TxnRead.Range()方法进行查询
	rr, err := txn.Range(r.Key, mkGteRange(r.RangeEnd), ro)
	if err != nil {
		return nil, err
	}

	if r.MaxModRevision != 0 {	//根据MaxModRevision对查询结果进行过滤
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision > r.MaxModRevision }
		pruneKVs(rr, f)
	}
	if r.MinModRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision < r.MinModRevision }
		pruneKVs(rr, f)			//该函数会根据上面定义的回调函数的返回值进行过滤
	}
	if r.MaxCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision > r.MaxCreateRevision }
		pruneKVs(rr, f)
	}
	if r.MinCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision < r.MinCreateRevision }
		pruneKVs(rr, f)
	}

	sortOrder := r.SortOrder	//根据SortOrder和SortTarget对结果集进行排序
	if r.SortTarget != pb.RangeRequest_KEY && sortOrder == pb.RangeRequest_NONE {
		// Since current mvcc.Range implementation returns results
		// sorted by keys in lexiographically ascending order,
		// sort ASCEND by default only when target is not 'KEY'
		sortOrder = pb.RangeRequest_ASCEND
	}
	if sortOrder != pb.RangeRequest_NONE {
		var sorter sort.Interface
		switch {
		case r.SortTarget == pb.RangeRequest_KEY:
			sorter = &kvSortByKey{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VERSION:
			sorter = &kvSortByVersion{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_CREATE:
			sorter = &kvSortByCreate{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_MOD:
			sorter = &kvSortByMod{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VALUE:
			sorter = &kvSortByValue{&kvSort{rr.KVs}}
		}
		switch {
		case sortOrder == pb.RangeRequest_ASCEND:
			sort.Sort(sorter)
		case sortOrder == pb.RangeRequest_DESCEND:
			sort.Sort(sort.Reverse(sorter))
		}
	}

	if r.Limit > 0 && len(rr.KVs) > int(r.Limit) {				//根据limit对结果集进行过滤
		rr.KVs = rr.KVs[:r.Limit]
		resp.More = true
	}

	resp.Header.Revision = rr.Rev								//将查询结果集中的键值对数据填充到RangeResponse中，并返回
	resp.Count = int64(rr.Count)								//此次返回的键值对个数
	resp.Kvs = make([]*mvccpb.KeyValue, len(rr.KVs))
	for i := range rr.KVs {
		if r.KeysOnly {											//只返回Key值
			rr.KVs[i].Value = nil
		}
		resp.Kvs[i] = &rr.KVs[i]
	}
	return resp, nil
}
//该方法提供了批量操作的功能。
func (a *applierV3backend) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	isWrite := !isTxnReadonly(rt)						//同事检测TxnRequest.Success和Failure中封装的操作，如果两者的全部操作都是读操作，则返回false
	//NewReadOnlyTxnWrite函数返回一个txnReadWrite实例
	txn := mvcc.NewReadOnlyTxnWrite(a.s.KV().Read())	//开启只读事务

	txnPath := compareToPath(txn, rt)
	if isWrite {
		if _, err := checkRequests(txn, rt, txnPath, a.checkPut); err != nil {
			txn.End()
			return nil, err
		}
	}
	if _, err := checkRequests(txn, rt, txnPath, a.checkRange); err != nil {
		txn.End()
		return nil, err
	}

	txnResp, _ := newTxnResp(rt, txnPath)

	// When executing mutable txn ops, etcd must hold the txn lock so
	// readers do not see any intermediate results. Since writes are
	// serialized on the raft loop, the revision in the read view will
	// be the revision of the write txn.
	if isWrite {							//如果TxnRequest中包含写操作，则结束当前只读事务，开启读写事务
		txn.End()
		txn = a.s.KV().Write()
	}
	a.applyTxn(txn, rt, txnPath, txnResp)	//执行真正的操作，并记录相应的返回值
	rev := txn.Rev()						//获取当前的revison
	if len(txn.Changes()) != 0 {
		rev++								//如果此次事务中有更新操作，则递增rev
	}
	txn.End()								//提交事务

	txnResp.Header.Revision = rev			//在返回值中记录最新的revision
	return txnResp, nil
}

// newTxnResp allocates a txn response for a txn request given a path.
func newTxnResp(rt *pb.TxnRequest, txnPath []bool) (txnResp *pb.TxnResponse, txnCount int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	resps := make([]*pb.ResponseOp, len(reqs))
	txnResp = &pb.TxnResponse{
		Responses: resps,
		Succeeded: txnPath[0],
		Header:    &pb.ResponseHeader{},
	}
	for i, req := range reqs {
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{}}
		case *pb.RequestOp_RequestPut:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{}}
		case *pb.RequestOp_RequestDeleteRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{}}
		case *pb.RequestOp_RequestTxn:
			resp, txns := newTxnResp(tv.RequestTxn, txnPath[1:])
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: resp}}
			txnPath = txnPath[1+txns:]
			txnCount += txns + 1
		default:
		}
	}
	return txnResp, txnCount
}

func compareToPath(rv mvcc.ReadView, rt *pb.TxnRequest) []bool {
	txnPath := make([]bool, 1)
	ops := rt.Success
	if txnPath[0] = applyCompares(rv, rt.Compare); !txnPath[0] {
		ops = rt.Failure
	}
	for _, op := range ops {
		tv, ok := op.Request.(*pb.RequestOp_RequestTxn)
		if !ok || tv.RequestTxn == nil {
			continue
		}
		txnPath = append(txnPath, compareToPath(rv, tv.RequestTxn)...)
	}
	return txnPath
}

func applyCompares(rv mvcc.ReadView, cmps []*pb.Compare) bool {
	for _, c := range cmps {
		if !applyCompare(rv, c) {
			return false
		}
	}
	return true
}

// applyCompare applies the compare request.
// If the comparison succeeds, it returns true. Otherwise, returns false.
func applyCompare(rv mvcc.ReadView, c *pb.Compare) bool {
	// TODO: possible optimizations
	// * chunk reads for large ranges to conserve memory
	// * rewrite rules for common patterns:
	//	ex. "[a, b) createrev > 0" => "limit 1 /\ kvs > 0"
	// * caching
	rr, err := rv.Range(c.Key, mkGteRange(c.RangeEnd), mvcc.RangeOptions{})			//查询指定键值对
	if err != nil {
		return false
	}
	if len(rr.KVs) == 0 {
		if c.Target == pb.Compare_VALUE {
			// Always fail if comparing a value on a key/keys that doesn't exist;
			// nil == empty string in grpc; no way to represent missing value
			return false
		}
		return compareKV(c, mvccpb.KeyValue{})
	}
	for _, kv := range rr.KVs {
		if !compareKV(c, kv) {
			return false
		}
	}
	return true
}

func compareKV(c *pb.Compare, ckv mvccpb.KeyValue) bool {
	var result int
	rev := int64(0)
	switch c.Target {								//Compare.Target指定了比较KLeyValue中的哪个字段，在其TargetUnion字段中记录了对应的值
	case pb.Compare_VALUE:
		v := []byte{}
		if tv, _ := c.TargetUnion.(*pb.Compare_Value); tv != nil {
			v = tv.Value
		}
		result = bytes.Compare(ckv.Value, v)		//比较Value值
	case pb.Compare_CREATE:
		if tv, _ := c.TargetUnion.(*pb.Compare_CreateRevision); tv != nil {
			rev = tv.CreateRevision
		}
		result = compareInt64(ckv.CreateRevision, rev)
	case pb.Compare_MOD:
		if tv, _ := c.TargetUnion.(*pb.Compare_ModRevision); tv != nil {
			rev = tv.ModRevision
		}
		result = compareInt64(ckv.ModRevision, rev)
	case pb.Compare_VERSION:
		if tv, _ := c.TargetUnion.(*pb.Compare_Version); tv != nil {
			rev = tv.Version
		}
		result = compareInt64(ckv.Version, rev)
	case pb.Compare_LEASE:
		if tv, _ := c.TargetUnion.(*pb.Compare_Lease); tv != nil {
			rev = tv.Lease
		}
		result = compareInt64(ckv.Lease, rev)
	}
	switch c.Result {						//在Compare.Result字段中记录了此次比较成功的条件
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	}
	return true
}

func (a *applierV3backend) applyTxn(txn mvcc.TxnWrite, rt *pb.TxnRequest, txnPath []bool, tresp *pb.TxnResponse) (txns int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	for i, req := range reqs {						//执行真正的操作，并记录相应的返回值
		respi := tresp.Responses[i].Response
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resp, err := a.Range(txn, tv.RequestRange)
			if err != nil {
				plog.Panicf("unexpected error during txn: %v", err)
			}
			respi.(*pb.ResponseOp_ResponseRange).ResponseRange = resp
		case *pb.RequestOp_RequestPut:
			resp, err := a.Put(txn, tv.RequestPut)
			if err != nil {
				plog.Panicf("unexpected error during txn: %v", err)
			}
			respi.(*pb.ResponseOp_ResponsePut).ResponsePut = resp
		case *pb.RequestOp_RequestDeleteRange:
			resp, err := a.DeleteRange(txn, tv.RequestDeleteRange)
			if err != nil {
				plog.Panicf("unexpected error during txn: %v", err)
			}
			respi.(*pb.ResponseOp_ResponseDeleteRange).ResponseDeleteRange = resp
		case *pb.RequestOp_RequestTxn:
			resp := respi.(*pb.ResponseOp_ResponseTxn).ResponseTxn
			applyTxns := a.applyTxn(txn, tv.RequestTxn, txnPath[1:], resp)
			txns += applyTxns + 1
			txnPath = txnPath[applyTxns+1:]
		default:
			// empty union
		}
	}
	return txns
}
//该方法用来处理CompactionRequest请求的，其底层调用Compact()方法完成压缩。
func (a *applierV3backend) Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, error) {
	resp := &pb.CompactionResponse{}
	resp.Header = &pb.ResponseHeader{}
	ch, err := a.s.KV().Compact(compaction.Revision)					//完成压缩操作
	if err != nil {
		return nil, ch, err
	}
	// get the current revision. which key to get is not important.		主要为了获取最新的revision
	rr, _ := a.s.KV().Range([]byte("compaction"), nil, mvcc.RangeOptions{})
	resp.Header.Revision = rr.Rev
	return resp, ch, err
}

func (a *applierV3backend) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	l, err := a.s.lessor.Grant(lease.LeaseID(lc.ID), lc.TTL)
	resp := &pb.LeaseGrantResponse{}
	if err == nil {
		resp.ID = int64(l.ID)
		resp.TTL = l.TTL()
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	err := a.s.lessor.Revoke(lease.LeaseID(lc.ID))
	return &pb.LeaseRevokeResponse{Header: newHeader(a.s)}, err
}
//当AlarmRequest请求经过Raft协议提交之后，会调用该方法应用相应的Entry记录，该方法会根据AlarmRequest.Action字段分类处理。
func (a *applierV3backend) Alarm(ar *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp := &pb.AlarmResponse{}
	oldCount := len(a.s.alarmStore.Get(ar.Alarm))

	switch ar.Action {
	case pb.AlarmRequest_GET:			//调用AlarmStore.Get()方法处理AlarmRequest_GET类型的Action
		resp.Alarms = a.s.alarmStore.Get(ar.Alarm)
	case pb.AlarmRequest_ACTIVATE:		//调用AlarmStore.Activate()方法处理AlarmRequest_ACTIVATE类型的Action
		m := a.s.alarmStore.Activate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		activated := oldCount == 0 && len(a.s.alarmStore.Get(m.Alarm)) == 1
		if !activated {
			break
		}

		plog.Warningf("alarm %v raised by peer %s", m.Alarm, types.ID(m.MemberID))
		switch m.Alarm {
		case pb.AlarmType_CORRUPT:
			a.s.applyV3 = newApplierV3Corrupt(a)
		case pb.AlarmType_NOSPACE:
			a.s.applyV3 = newApplierV3Capped(a)
		default:
			plog.Errorf("unimplemented alarm activation (%+v)", m)
		}
	case pb.AlarmRequest_DEACTIVATE:		//调用AlarmStore.Activate()方法处理AlarmRequest_DEACTIVATE类型的Action
		m := a.s.alarmStore.Deactivate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		deactivated := oldCount > 0 && len(a.s.alarmStore.Get(ar.Alarm)) == 0
		if !deactivated {
			break
		}

		switch m.Alarm {
		case pb.AlarmType_NOSPACE, pb.AlarmType_CORRUPT:
			// TODO: check kv hash before deactivating CORRUPT?
			plog.Infof("alarm disarmed %+v", ar)
			a.s.applyV3 = a.s.newApplierV3()
		default:
			plog.Errorf("unimplemented alarm deactivation (%+v)", m)
		}
	default:
		return nil, nil
	}
	return resp, nil
}
//在quotaApplierV3触发限流操作之后，就会创建applierV3Capped实例替换EtcdServer当前使用的applierV3接口实现。通过该实例执行的任何写入操作都会失败，这样就可以保证底层
//存储中的数据量不再增加
type applierV3Capped struct {
	applierV3
	q backendQuota
}

// newApplierV3Capped creates an applyV3 that will reject Puts and transactions
// with Puts so that the number of keys in the store is capped.
func newApplierV3Capped(base applierV3) applierV3 { return &applierV3Capped{applierV3: base} }

func (a *applierV3Capped) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, error) {
	return nil, ErrNoSpace
}

func (a *applierV3Capped) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if a.q.Cost(r) > 0 {
		return nil, ErrNoSpace
	}
	return a.applierV3.Txn(r)
}

func (a *applierV3Capped) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	return nil, ErrNoSpace
}

func (a *applierV3backend) AuthEnable() (*pb.AuthEnableResponse, error) {
	err := a.s.AuthStore().AuthEnable()
	if err != nil {
		return nil, err
	}
	return &pb.AuthEnableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) AuthDisable() (*pb.AuthDisableResponse, error) {
	a.s.AuthStore().AuthDisable()
	return &pb.AuthDisableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error) {
	ctx := context.WithValue(context.WithValue(a.s.ctx, auth.AuthenticateParamIndex{}, a.s.consistIndex.ConsistentIndex()), auth.AuthenticateParamSimpleTokenPrefix{}, r.SimpleToken)
	resp, err := a.s.AuthStore().Authenticate(ctx, r.Name, r.Password)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := a.s.AuthStore().UserAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := a.s.AuthStore().UserDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := a.s.AuthStore().UserChangePassword(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := a.s.AuthStore().UserGrantRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := a.s.AuthStore().UserGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := a.s.AuthStore().UserRevokeRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := a.s.AuthStore().RoleAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleGrantPermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := a.s.AuthStore().RoleGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleRevokePermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := a.s.AuthStore().RoleDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := a.s.AuthStore().UserList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := a.s.AuthStore().RoleList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}
//在applierV3backend的基础上提供了限流功能，即底层的BoltDB数据库文件的大小增大到上限之后，就会触发限流操作。
type quotaApplierV3 struct {
	applierV3
	q Quota
}

func newQuotaApplierV3(s *EtcdServer, app applierV3) applierV3 {
	return &quotaApplierV3{app, NewBackendQuota(s)}
}
//该方法首先会调用Quota.Available()方法检测此次请求是否会触发限流，如果未触发，则委托给底层的applierV3实现完成真正的PUT操作。
func (a *quotaApplierV3) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, error) {
	ok := a.q.Available(p)					//检测是否触发限流
	resp, err := a.applierV3.Put(txn, p)	//完成真正的Put操作
	if err == nil && !ok {
		err = ErrNoSpace					//注意返回的错误信息
	}
	return resp, err
}

func (a *quotaApplierV3) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	ok := a.q.Available(rt)
	resp, err := a.applierV3.Txn(rt)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, err
}

func (a *quotaApplierV3) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	ok := a.q.Available(lc)
	resp, err := a.applierV3.LeaseGrant(lc)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, err
}

type kvSort struct{ kvs []mvccpb.KeyValue }

func (s *kvSort) Swap(i, j int) {
	t := s.kvs[i]
	s.kvs[i] = s.kvs[j]
	s.kvs[j] = t
}
func (s *kvSort) Len() int { return len(s.kvs) }

type kvSortByKey struct{ *kvSort }

func (s *kvSortByKey) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Key, s.kvs[j].Key) < 0
}

type kvSortByVersion struct{ *kvSort }

func (s *kvSortByVersion) Less(i, j int) bool {
	return (s.kvs[i].Version - s.kvs[j].Version) < 0
}

type kvSortByCreate struct{ *kvSort }

func (s *kvSortByCreate) Less(i, j int) bool {
	return (s.kvs[i].CreateRevision - s.kvs[j].CreateRevision) < 0
}

type kvSortByMod struct{ *kvSort }

func (s *kvSortByMod) Less(i, j int) bool {
	return (s.kvs[i].ModRevision - s.kvs[j].ModRevision) < 0
}

type kvSortByValue struct{ *kvSort }

func (s *kvSortByValue) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Value, s.kvs[j].Value) < 0
}

func checkRequests(rv mvcc.ReadView, rt *pb.TxnRequest, txnPath []bool, f checkReqFunc) (int, error) {
	txnCount := 0
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	for _, req := range reqs {
		if tv, ok := req.Request.(*pb.RequestOp_RequestTxn); ok && tv.RequestTxn != nil {
			txns, err := checkRequests(rv, tv.RequestTxn, txnPath[1:], f)
			if err != nil {
				return 0, err
			}
			txnCount += txns + 1
			txnPath = txnPath[txns+1:]
			continue
		}
		if err := f(rv, req); err != nil {
			return 0, err
		}
	}
	return txnCount, nil
}

func (a *applierV3backend) checkRequestPut(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestPut)
	if !ok || tv.RequestPut == nil {
		return nil
	}
	req := tv.RequestPut
	if req.IgnoreValue || req.IgnoreLease {
		// expects previous key-value, error if not exist
		rr, err := rv.Range(req.Key, nil, mvcc.RangeOptions{})
		if err != nil {
			return err
		}
		if rr == nil || len(rr.KVs) == 0 {
			return ErrKeyNotFound
		}
	}
	if lease.LeaseID(req.Lease) != lease.NoLease {
		if l := a.s.lessor.Lookup(lease.LeaseID(req.Lease)); l == nil {
			return lease.ErrLeaseNotFound
		}
	}
	return nil
}

func (a *applierV3backend) checkRequestRange(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestRange)
	if !ok || tv.RequestRange == nil {
		return nil
	}
	req := tv.RequestRange
	switch {
	case req.Revision == 0:
		return nil
	case req.Revision > rv.Rev():
		return mvcc.ErrFutureRev
	case req.Revision < rv.FirstRev():
		return mvcc.ErrCompacted
	}
	return nil
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// mkGteRange determines if the range end is a >= range. This works around grpc
// sending empty byte strings as nil; >= is encoded in the range end as '\0'.
// If it is a GTE range, then []byte{} is returned to indicate the empty byte
// string (vs nil being no byte string).
func mkGteRange(rangeEnd []byte) []byte {
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		return []byte{}
	}
	return rangeEnd
}

func noSideEffect(r *pb.InternalRaftRequest) bool {
	return r.Range != nil || r.AuthUserGet != nil || r.AuthRoleGet != nil
}

func removeNeedlessRangeReqs(txn *pb.TxnRequest) {
	f := func(ops []*pb.RequestOp) []*pb.RequestOp {
		j := 0
		for i := 0; i < len(ops); i++ {
			if _, ok := ops[i].Request.(*pb.RequestOp_RequestRange); ok {
				continue
			}
			ops[j] = ops[i]
			j++
		}

		return ops[:j]
	}

	txn.Success = f(txn.Success)
	txn.Failure = f(txn.Failure)
}

func pruneKVs(rr *mvcc.RangeResult, isPrunable func(*mvccpb.KeyValue) bool) {
	j := 0
	for i := range rr.KVs {
		rr.KVs[j] = rr.KVs[i]
		if !isPrunable(&rr.KVs[i]) {
			j++
		}
	}
	rr.KVs = rr.KVs[:j]
}

func newHeader(s *EtcdServer) *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: uint64(s.Cluster().ID()),
		MemberId:  uint64(s.ID()),
		Revision:  s.KV().Rev(),
		RaftTerm:  s.Term(),
	}
}
