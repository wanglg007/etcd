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
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/coreos/etcd/auth"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/lease/leasehttp"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/raft"

	"github.com/gogo/protobuf/proto"
)

const (
	// In the health case, there might be a small gap (10s of entries) between
	// the applied index and committed index.
	// However, if the committed entries are very heavy to apply, the gap might grow.
	// We should stop accepting new proposals if the gap growing to a certain point.
	maxGapBetweenApplyAndCommitIndex = 5000
)

type RaftKV interface {
	Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)
	Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error)
}

type Lessor interface {
	// LeaseGrant sends LeaseGrant request to raft and apply it after committed.
	LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	// LeaseRevoke sends LeaseRevoke request to raft and apply it after committed.
	LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	// LeaseRenew renews the lease with given ID. The renewed TTL is returned. Or an error
	// is returned.
	LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error)

	// LeaseTimeToLive retrieves lease information.
	LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error)

	// LeaseLeases lists all leases.
	LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error)
}

type Authenticator interface {
	AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error)
	AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error)
	Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error)
	UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	var resp *pb.RangeResponse
	var err error
	defer func(start time.Time) {
		warnOfExpensiveReadOnlyRangeRequest(start, r, resp, err)
	}(time.Now())

	if !r.Serializable {
		err = s.linearizableReadNotify(ctx)
		if err != nil {
			return nil, err
		}
	}
	chk := func(ai *auth.AuthInfo) error {
		return s.authStore.IsRangePermitted(ai, r.Key, r.RangeEnd)
	}

	get := func() { resp, err = s.applyV3Base.Range(nil, r) }
	if serr := s.doSerialize(ctx, chk, get); serr != nil {
		err = serr
		return nil, err
	}
	return resp, err
}

func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Put: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PutResponse), nil
}

func (s *EtcdServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{DeleteRange: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteRangeResponse), nil
}

func (s *EtcdServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if isTxnReadonly(r) {
		if !isTxnSerializable(r) {
			err := s.linearizableReadNotify(ctx)
			if err != nil {
				return nil, err
			}
		}
		var resp *pb.TxnResponse
		var err error
		chk := func(ai *auth.AuthInfo) error {
			return checkTxnAuth(s.authStore, ai, r)
		}

		defer func(start time.Time) {
			warnOfExpensiveReadOnlyTxnRequest(start, r, resp, err)
		}(time.Now())

		get := func() { resp, err = s.applyV3Base.Txn(r) }
		if serr := s.doSerialize(ctx, chk, get); serr != nil {
			return nil, serr
		}
		return resp, err
	}

	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Txn: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	return true
}

func isTxnReadonly(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	return true
}

func (s *EtcdServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	result, err := s.processInternalRaftRequestOnce(ctx, pb.InternalRaftRequest{Compaction: r})
	if r.Physical && result != nil && result.physc != nil {
		<-result.physc
		// The compaction is done deleting keys; the hash is now settled
		// but the data is not necessarily committed. If there's a crash,
		// the hash may revert to a hash prior to compaction completing
		// if the compaction resumes. Force the finished compaction to
		// commit so it won't resume following a crash.
		s.be.ForceCommit()
	}
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	resp := result.resp.(*pb.CompactionResponse)
	if resp == nil {
		resp = &pb.CompactionResponse{}
	}
	if resp.Header == nil {
		resp.Header = &pb.ResponseHeader{}
	}
	resp.Header.Revision = s.kv.Rev()
	return resp, nil
}

func (s *EtcdServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	// no id given? choose one
	for r.ID == int64(lease.NoLease) {
		// only use positive int64 id's
		r.ID = int64(s.reqIDGen.Next() & ((1 << 63) - 1))
	}
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseGrant: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LeaseGrantResponse), nil
}
//该方法会将LeaseRevokeRequest请求中的信息封装成MsgProp消息，并发送到集群中的其他节点。
func (s *EtcdServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	//将LeaseRevokeRequest封装成InternalRaftRequest，并调用raftRequestOnce()方法
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseRevoke: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LeaseRevokeResponse), nil
}

func (s *EtcdServer) LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error) {
	ttl, err := s.lessor.Renew(id)
	if err == nil { // already requested to primary lessor(leader)
		return ttl, nil
	}
	if err != lease.ErrNotPrimary {
		return -1, err
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// renewals don't go through raft; forward to leader manually
	for cctx.Err() == nil && err != nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return -1, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeasePrefix
			ttl, err = leasehttp.RenewHTTP(cctx, id, lurl, s.peerRt)
			if err == nil || err == lease.ErrLeaseNotFound {
				return ttl, err
			}
		}
	}
	return -1, ErrTimeout
}

func (s *EtcdServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	if s.Leader() == s.ID() {
		// primary; timetolive directly from leader
		le := s.lessor.Lookup(lease.LeaseID(r.ID))
		if le == nil {
			return nil, lease.ErrLeaseNotFound
		}
		// TODO: fill out ResponseHeader
		resp := &pb.LeaseTimeToLiveResponse{Header: &pb.ResponseHeader{}, ID: r.ID, TTL: int64(le.Remaining().Seconds()), GrantedTTL: le.TTL()}
		if r.Keys {
			ks := le.Keys()
			kbs := make([][]byte, len(ks))
			for i := range ks {
				kbs[i] = []byte(ks[i])
			}
			resp.Keys = kbs
		}
		return resp, nil
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// forward to leader
	for cctx.Err() == nil {
		leader, err := s.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeaseInternalPrefix
			resp, err := leasehttp.TimeToLiveHTTP(cctx, lease.LeaseID(r.ID), r.Keys, lurl, s.peerRt)
			if err == nil {
				return resp.LeaseTimeToLiveResponse, nil
			}
			if err == lease.ErrLeaseNotFound {
				return nil, err
			}
		}
	}
	return nil, ErrTimeout
}

func (s *EtcdServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	ls := s.lessor.Leases()
	lss := make([]*pb.LeaseStatus, len(ls))
	for i := range ls {
		lss[i] = &pb.LeaseStatus{ID: int64(ls[i].ID)}
	}
	return &pb.LeaseLeasesResponse{Header: newHeader(s), Leases: lss}, nil
}

func (s *EtcdServer) waitLeader(ctx context.Context) (*membership.Member, error) {
	leader := s.cluster.Member(s.Leader())
	for leader == nil {
		// wait an election
		dur := time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond
		select {
		case <-time.After(dur):
			leader = s.cluster.Member(s.Leader())
		case <-s.stopping:
			return nil, ErrStopped
		case <-ctx.Done():
			return nil, ErrNoLeader
		}
	}
	if leader == nil || len(leader.PeerURLs) == 0 {
		return nil, ErrNoLeader
	}
	return leader, nil
}

func (s *EtcdServer) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{Alarm: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AlarmResponse), nil
}

func (s *EtcdServer) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{AuthEnable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthEnableResponse), nil
}

func (s *EtcdServer) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthDisable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthDisableResponse), nil
}

func (s *EtcdServer) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	if err := s.linearizableReadNotify(ctx); err != nil {
		return nil, err
	}

	var resp proto.Message
	for {
		checkedRevision, err := s.AuthStore().CheckPassword(r.Name, r.Password)
		if err != nil {
			if err != auth.ErrAuthNotEnabled {
				plog.Errorf("invalid authentication request to user %s was issued", r.Name)
			}
			return nil, err
		}

		st, err := s.AuthStore().GenTokenPrefix()
		if err != nil {
			return nil, err
		}

		internalReq := &pb.InternalAuthenticateRequest{
			Name:        r.Name,
			Password:    r.Password,
			SimpleToken: st,
		}

		resp, err = s.raftRequestOnce(ctx, pb.InternalRaftRequest{Authenticate: internalReq})
		if err != nil {
			return nil, err
		}
		if checkedRevision == s.AuthStore().Revision() {
			break
		}
		plog.Infof("revision when password checked is obsolete, retrying")
	}

	return resp.(*pb.AuthenticateResponse), nil
}

func (s *EtcdServer) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserAddResponse), nil
}

func (s *EtcdServer) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserDeleteResponse), nil
}

func (s *EtcdServer) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserChangePassword: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserChangePasswordResponse), nil
}

func (s *EtcdServer) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGrantRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGrantRoleResponse), nil
}

func (s *EtcdServer) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGetResponse), nil
}

func (s *EtcdServer) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserListResponse), nil
}

func (s *EtcdServer) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserRevokeRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserRevokeRoleResponse), nil
}

func (s *EtcdServer) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleAddResponse), nil
}

func (s *EtcdServer) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGrantPermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGrantPermissionResponse), nil
}

func (s *EtcdServer) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGetResponse), nil
}

func (s *EtcdServer) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleListResponse), nil
}

func (s *EtcdServer) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleRevokePermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleRevokePermissionResponse), nil
}

func (s *EtcdServer) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleDeleteResponse), nil
}

func (s *EtcdServer) raftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	result, err := s.processInternalRaftRequestOnce(ctx, r)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.resp, nil
}

func (s *EtcdServer) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	for {
		resp, err := s.raftRequestOnce(ctx, r)
		if err != auth.ErrAuthOldRevision {
			return resp, err
		}
	}
}

// doSerialize handles the auth logic, with permissions checked by "chk", for a serialized request "get". Returns a non-nil error on authentication failure.
func (s *EtcdServer) doSerialize(ctx context.Context, chk func(*auth.AuthInfo) error, get func()) error {
	for {
		ai, err := s.AuthInfoFromCtx(ctx)
		if err != nil {
			return err
		}
		if ai == nil {
			// chk expects non-nil AuthInfo; use empty credentials
			ai = &auth.AuthInfo{}
		}
		if err = chk(ai); err != nil {
			if err == auth.ErrAuthOldRevision {
				continue
			}
			return err
		}
		// fetch response for serialized request
		get()
		//  empty credentials or current auth info means no need to retry
		if ai.Revision == 0 || ai.Revision == s.authStore.Revision() {
			return nil
		}
		// avoid TOCTOU error, retry of the request is required.
	}
}

func (s *EtcdServer) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (*applyResult, error) {
	ai := s.getAppliedIndex()
	ci := s.getCommittedIndex()
	if ci > ai+maxGapBetweenApplyAndCommitIndex {		//检测当前是否有大量已提交但未应用的Entry记录，如果有，则返回错误实现限流
		return nil, ErrTooManyRequests
	}

	r.Header = &pb.RequestHeader{						//为请求生成id
		ID: s.reqIDGen.Next(),
	}

	authInfo, err := s.AuthInfoFromCtx(ctx)				//获取权限信息，记录到请求头中
	if err != nil {
		return nil, err
	}
	if authInfo != nil {
		r.Header.Username = authInfo.Username
		r.Header.AuthRevision = authInfo.Revision
	}

	data, err := r.Marshal()							//将InternalRaftRequest请求序列化
	if err != nil {
		return nil, err
	}

	if len(data) > int(s.Cfg.MaxRequestBytes) {			//检测序列化后的长度
		return nil, ErrRequestTooLarge
	}

	id := r.ID
	if id == 0 {
		id = r.Header.ID
	}
	ch := s.w.Register(id)								//在EtcdServer.wait中未该请求注册相应的ch通道

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	start := time.Now()
	//将InternalRaftRequest序列化后的数据封装为Entry，之后生成MsgProp消息并发送出去
	s.r.Propose(cctx, data)
	proposalsPending.Inc()
	defer proposalsPending.Dec()

	select {
	case x := <-ch:										//阻塞等待上面发送的Entry被应用
		return x.(*applyResult), nil
	case <-cctx.Done():
		proposalsFailed.Inc()
		s.w.Trigger(id, nil) // GC wait
		return nil, s.parseProposeCtxErr(cctx.Err(), start)
	case <-s.done:
		return nil, ErrStopped
	}
}

// Watchable returns a watchable interface attached to the etcdserver.
func (s *EtcdServer) Watchable() mvcc.WatchableKV { return s.KV() }
//该方法首先会阻塞等待readwaitc通道上的信号。然后记录当前的EtcdServer.readNotifier字段并进行更新。之后发送MsgReadIndex消息并交由etcd-raft模块进行处理。
func (s *EtcdServer) linearizableReadLoop() {
	var rs raft.ReadState

	for {
		ctxToSend := make([]byte, 8)
		id1 := s.reqIDGen.Next()
		binary.BigEndian.PutUint64(ctxToSend, id1)			//创建唯一id

		select {
		//在Client发起一次Linearizable Read时，会向readwaitc通道中写入一个空结构体作为信号
		case <-s.readwaitc:			//监听到stopping通道关闭，则表示当前EtcdServer实例正在关闭，会结束该goroutine
		case <-s.stopping:
			return
		}
		//记录当前notifier实例，新建notifier实例，并更新EtcdServer.readNotifier字段
		nextnr := newNotifier()

		s.readMu.Lock()
		nr := s.readNotifier
		s.readNotifier = nextnr
		s.readMu.Unlock()

		cctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
		if err := s.r.ReadIndex(cctx, ctxToSend); err != nil {					//创建并处理MsgReadIndex请求
			cancel()
			if err == raft.ErrStopped {
				return
			}
			plog.Errorf("failed to get read index from raft: %v", err)
			readIndexFailed.Inc()
			nr.notify(err)
			continue
		}
		cancel()

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {									//检测MsgReadIndex请求是否超时，是否已被处理完
			select {
			//在raftNode处理Ready实例时，会将Ready.ReadStates中最后一项写入该通道中
			case rs = <-s.r.readStateC:
				done = bytes.Equal(rs.RequestCtx, ctxToSend)	//在ReadState.RequestCtx中携带了请求id
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					id2 := uint64(0)
					if len(rs.RequestCtx) == 8 {
						id2 = binary.BigEndian.Uint64(rs.RequestCtx)
					}
					//忽略乱序的响应，则输出日志并继续当前for循环等待请求对应的响应
					plog.Warningf("ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader (request ID want %d, got %d)", id1, id2)
					slowReadIndex.Inc()
				}

			case <-time.After(s.Cfg.ReqTimeout()):
				plog.Warningf("timed out waiting for read index response (local node might have slow network)")
				nr.notify(ErrTimeout)							//在notifier.err字段中设置错误信息，同时关闭notifier.c通道
				timeout = true
				slowReadIndex.Inc()

			case <-s.stopping:
				return
			}
		}
		if !done {
			continue
		}

		if ai := s.getAppliedIndex(); ai < rs.Index {		//ReadState.Index记录的是commitIndex
			select {
			//如果当前节点已应用的Entry记录未到指定的committed Index，则需要阻塞等待。后面EtcdServer.applyAll()方法处理完待应用Entry记录之后，会将已应用Entry记录
			//在WaitTime中的通道关闭，则linearizableReadLoop goroutine可以得到此信号，继续执行。
			case <-s.applyWait.Wait(rs.Index):
			case <-s.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.notify(nil)									//关闭notifier.c通道
	}
}

func (s *EtcdServer) linearizableReadNotify(ctx context.Context) error {
	s.readMu.RLock()
	nc := s.readNotifier						//获取EtcdServer.readNotifier实例
	s.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	//linearizableReadLoop goroutine中会阻塞监听readwaitc通道，这里会向readwaitc通道中写入一个空结构体作为信号，通知linearizableReadLoop goroutine开始工作
	case s.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	//当linearizableReadLoop goroutine发现已应用Entry的索引值超过了请求时的committedIndex，则关闭notified.c通道，通知执行读取操作的goroutine继续后续读取操作
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStopped
	}
}

func (s *EtcdServer) AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	authInfo, err := s.AuthStore().AuthInfoFromCtx(ctx)
	if authInfo != nil || err != nil {
		return authInfo, err
	}
	if !s.Cfg.ClientCertAuthEnabled {
		return nil, nil
	}
	authInfo = s.AuthStore().AuthInfoFromTLS(ctx)
	return authInfo, nil
}
