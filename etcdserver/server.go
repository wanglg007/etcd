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
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/alarm"
	"github.com/coreos/etcd/auth"
	"github.com/coreos/etcd/compactor"
	"github.com/coreos/etcd/discovery"
	"github.com/coreos/etcd/etcdserver/api"
	"github.com/coreos/etcd/etcdserver/api/v2http/httptypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/lease/leasehttp"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/runtime"
	"github.com/coreos/etcd/pkg/schedule"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/store"
	"github.com/coreos/etcd/version"
	"github.com/coreos/etcd/wal"

	"github.com/coreos/go-semver/semver"
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DefaultSnapCount = 100000

	StoreClusterPrefix = "/0"
	StoreKeysPrefix    = "/1"

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	purgeFileInterval = 30 * time.Second
	// monitorVersionInterval should be smaller than the timeout
	// on the connection. Or we will not be able to reuse the connection
	// (since it will timeout).
	monitorVersionInterval = rafthttp.ConnWriteTimeout - time.Second

	// max number of in-flight snapshot messages etcdserver allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16

	releaseDelayAfterSnapshot = 30 * time.Second

	// maxPendingRevokes is the maximum number of outstanding expired lease revocations.
	maxPendingRevokes = 16

	recommendedMaxRequestBytes = 10 * 1024 * 1024
)

var (
	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "etcdserver")

	storeMemberAttributeRegexp = regexp.MustCompile(path.Join(membership.StoreMembersPrefix, "[[:xdigit:]]{1,16}", "attributes"))
)

func init() {
	rand.Seed(time.Now().UnixNano())

	expvar.Publish(
		"file_descriptor_limit",
		expvar.Func(
			func() interface{} {
				n, _ := runtime.FDLimit()
				return n
			},
		),
	)
}

type Response struct {
	Term    uint64
	Index   uint64
	Event   *store.Event
	Watcher store.Watcher
	Err     error
}

type ServerV2 interface {
	Server
	// Do takes a V2 request and attempts to fulfill it, returning a Response.
	Do(ctx context.Context, r pb.Request) (Response, error)
	stats.Stats
	ClientCertAuthEnabled() bool
}

type ServerV3 interface {
	Server
	ID() types.ID
	RaftTimer
}

func (s *EtcdServer) ClientCertAuthEnabled() bool { return s.Cfg.ClientCertAuthEnabled }
//该结构体是etcd服务端的核心接口，其中定义了etcd服务端的主要功能
type Server interface {
	// Leader returns the ID of the leader Server.								获取当前集群中的Leader的ID
	Leader() types.ID

	// AddMember attempts to add a member into the cluster. It will return
	// ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDExists if member ID exists in the cluster.
	AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)	//向当前etcd集群中添加一个节点
	// RemoveMember attempts to remove a member from the cluster. It will
	// return ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDNotFound if member ID is not in the cluster.
	RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)				//从当前etcd集群中删除一个节点
	// UpdateMember attempts to update an existing member in the cluster. It will			修改集群成员属性，如果成员ID不存在则返回错误
	// return ErrIDNotFound if the member ID does not exist.
	UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error)

	// ClusterVersion is the cluster-wide minimum major.minor version.
	// Cluster version is set to the min version that an etcd member is
	// compatible with when first bootstrap.
	//
	// ClusterVersion is nil until the cluster is bootstrapped (has a quorum).
	//
	// During a rolling upgrades, the ClusterVersion will be updated
	// automatically after a sync. (5 second by default)
	//
	// The API/raft component can utilize ClusterVersion to determine if
	// it can accept a client request or a raft RPC.
	// NOTE: ClusterVersion might be nil when etcd 2.1 works with etcd 2.0 and
	// the leader is etcd 2.0. etcd 2.0 leader will not update clusterVersion since
	// this feature is introduced post 2.0.
	ClusterVersion() *semver.Version
	Cluster() api.Cluster
	Alarms() []*pb.AlarmMember
}

// EtcdServer is the production implementation of the Server interface
type EtcdServer struct {
	// inflightSnapshots holds count the number of snapshots currently inflight.
	inflightSnapshots int64  // must use atomic operations to access; keep 64-bit aligned.		当前已发送出去但未收到响应的快照个数
	appliedIndex      uint64 // must use atomic operations to access; keep 64-bit aligned.		当前节点已应用的Entry记录的最大索引值
	committedIndex    uint64 // must use atomic operations to access; keep 64-bit aligned.		当前已提交的Entry记录的索引值
	// consistIndex used to hold the offset of current executing entry
	// It is initialized to 0 before executing any entry.
	consistIndex consistentIndex // must use atomic operations to access; keep 64-bit aligned.
	r            raftNode        // uses 64-bit atomics; keep 64-bit aligned.					EtcdServer实例与底层etcd-raft模块通信的桥梁

	//当前节点将自身的信息推送到集群中其他节点之后，会将该通过关闭，也作为EtcdServer实例，可以对外提供服务的一个信号
	readych chan struct{}
	Cfg     ServerConfig					//封装了配置信息

	w wait.Wait								//Wait主要负责协调多个后台goroutine之间的执行。

	readMu sync.RWMutex
	// read routine notifies etcd server that it waits for reading by sending an empty struct to
	// readwaitC
	readwaitc chan struct{}
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	readNotifier *notifier

	//EtcdServer.start()方法启动会启动多个后台goroutine，其中一个后台goroutine执行EtcdServer.run()方法，监听stop通道。在EtcdServer.Stop()方法中会将stop通道关闭，
	//触发该run goroutine的结束。在run goroutine结束之前还会关闭stopping和done通道，从而触发其他后台goroutine的关闭。
	// stop signals the run goroutine should shutdown.
	stop chan struct{}
	// stopping is closed by run goroutine on shutdown.
	stopping chan struct{}
	// done is closed when all goroutines from start() complete.
	done chan struct{}

	errorc     chan error
	id         types.ID					//记录当前节点的ID
	attributes membership.Attributes	//记录当前节点的名称及接收集群中其他节点请求的URL地址

	cluster *membership.RaftCluster		//记录当前集群中全部节点的信息

	store       store.Store				//etcd v2版本存储
	snapshotter *snap.Snapshotter		//用来读写快照文件

	applyV2 ApplierV2											//applierv2接口是应用V2版本的Entry记录，其底层封装了V2存储

	// applyV3 is the applier with auth and quotas				applierV3接口主要是应用V3版本的Entry记录，其底层封装V3存储
	applyV3 applierV3
	// applyV3Base is the core applier without auth or quotas
	applyV3Base applierV3
	applyWait   wait.WaitTime

	kv         mvcc.ConsistentWatchableKV						//etcd v3版本的存储
	lessor     lease.Lessor
	bemu       sync.Mutex
	be         backend.Backend			//etcd V3版本的后端存储
	authStore  auth.AuthStore			//用于记录权限控制相关的信息
	alarmStore *alarm.AlarmStore		//用于记录报警相关的信息

	stats  *stats.ServerStats
	lstats *stats.LeaderStats

	SyncTicker *time.Ticker				//用来控制Leader节点定期发送SYNC消息的频率
	// compactor is used to auto-compact the KV.
	compactor compactor.Compactor		//Leader节点会对存储进行定期压缩，该字段用于控制定期压缩的频率

	// peerRt used to send requests (version, lease) to peers.
	peerRt   http.RoundTripper
	reqIDGen *idutil.Generator			//用于生成请求的唯一标识

	// forceVersionC is used to force the version monitor loop
	// to detect the cluster version immediately.
	forceVersionC chan struct{}

	// wgMu blocks concurrent waitgroup mutation while server stopping
	wgMu sync.RWMutex
	// wg is used to wait for the go routines that depends on the server state	在EtcdServer.Stop()方法中会通过该字段等待所有的后台goroutine全部退出
	// to exit when stopping the server.
	wg sync.WaitGroup

	// ctx is used for etcd-initiated requests that may need to be canceled
	// on etcd server shutdown.
	ctx    context.Context
	cancel context.CancelFunc

	leadTimeMu      sync.RWMutex
	leadElectedTime time.Time			//记录当前节点最后一次转换成Leader状态的时间戳
}
// 该函数会完成EtcdServer的初始化，也是etcd服务端生命周期的起始。其初始化的大致流程如下：(1)定义初始化过程中使用的变量，创建当前节点使用的目录；(2)根据配置
// 项初始化etcd-raft模块使用到的相关组件，例如，检测当前wal目录下是否存在WAL日志文件、初始化V2存储、查找BoltDB数据库文件、创建Backend实例、创建RoundTripper
// 实例等。(3)根据前面对WAL日志文件的查找结果及当期节点启动时的配置信息，初始化etcd-raft模块中的Node实例。(4)创建EtcdServer实例，并初始化其各个字段；
// NewServer creates a new EtcdServer from the supplied configuration. The
// configuration is considered static for the lifetime of the EtcdServer.
func NewServer(cfg ServerConfig) (srv *EtcdServer, err error) {
	st := store.New(StoreClusterPrefix, StoreKeysPrefix)

	var (
		w  *wal.WAL						//用于管理WAL日志文件的WAL实例
		n  raft.Node					//etcd-raft模块中的Node实例
		s  *raft.MemoryStorage			//MemoryStorage实例
		id types.ID						//记录当前节点的ID
		cl *membership.RaftCluster		//当前集群中的所有成员的信息
	)

	if cfg.MaxRequestBytes > recommendedMaxRequestBytes {
		plog.Warningf("MaxRequestBytes %v exceeds maximum recommended size %v", cfg.MaxRequestBytes, recommendedMaxRequestBytes)
	}
	//每个etcd节点都有会将其数据保存到“节点名称.etcd/member”目录下。如果在下面没有特殊说明，则提到的目录都是该目录下的子目录。这里会检测该目录是否存在，
	//如果不存在就创建该目录。
	if terr := fileutil.TouchDirAll(cfg.DataDir); terr != nil {		//确定snap目录是否存在，该目录是用来存放快照文件的。
		return nil, fmt.Errorf("cannot access data directory: %v", terr)
	}

	haveWAL := wal.Exist(cfg.WALDir())								//检测wal目录下是否存在WAL日志文件
	//创建V2版本存储
	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		plog.Fatalf("create snapshot directory error: %v", err)
	}
	ss := snap.New(cfg.SnapDir())									//创建Snapshotter实例，用来读写snap目录下的快照文件

	bepath := cfg.backendPath()										//获取BoltDB数据库存放的路径
	beExist := fileutil.Exist(bepath)								//检测BoltDB数据库文件是否存在
	be := openBackend(cfg)											//创建Backend实例，其中会单独启动一个后台goroutine来创建Backend实例

	defer func() {
		if err != nil {
			be.Close()
		}
	}()
	//根据配置创建RoundTripper实例，它主要负责实现网络请求等功能。
	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.peerDialTimeout())
	if err != nil {
		return nil, err
	}
	var (
		remotes  []*membership.Member
		snapshot *raftpb.Snapshot
	)

	switch {
	//场景1：即当前节点的wal目录不存在WAL日志文件，当前节点正在加入一个正在运行的集群
	case !haveWAL && !cfg.NewCluster:							//没有WAL日志文件且当前节点正在加入一个正在运行的集群
		//对配置的合法性进行检测，其中涉及配置信息中是否包含当前节点的相关信息，以及集群各个节点暴露的URL地址是否重复等。
		if err = cfg.VerifyJoinExisting(); err != nil {
			return nil, err
		}
		//根据配置信息，创建RaftCluster实例和其中的Member实例
		cl, err = membership.NewClusterFromURLsMap(cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		//GetClusterFromRemotePeers函数会过滤当前节点的信息，然后排序集群中其他节点暴露的URL地址并返回GetClusterFromRemotePeers从集群中其他节点请求集群信息
		//并创建相应的RaftCluster实例，然后将其与getRemotePeerURLs()返回值比较
		existingCluster, gerr := GetClusterFromRemotePeers(getRemotePeerURLs(cl, cfg.Name), prt)
		if gerr != nil {
			return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		if err = membership.ValidateClusterAndAssignIDs(cl, existingCluster); err != nil {
			return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}
		if !isCompatibleWithCluster(cl, cl.MemberByName(cfg.Name).ID, prt) {
			return nil, fmt.Errorf("incompatible with current running cluster")
		}

		remotes = existingCluster.Members()					//检测正在运行的集群与当前节点的版本，保证其版本相互兼容
		cl.SetID(existingCluster.ID())						//更新本地RaftCluster实例中的集群ID
		cl.SetStore(st)										//设置本地RaftCluster实例中的store字段
		cl.SetBackend(be)									//设置本地RaftCluster实例中的be字段，同时会在BoltDB中初始化后续用到的Bucket("member","members_removed","cluster")
		cfg.Print()
		id, n, s, w = startNode(cfg, cl, nil)			//调用startNode()函数，初始化raft.Node实例及相关组件
	//场景2：当前节点的wal目录下不存在WAL日志文件且当前集群是新建的。
	case !haveWAL && cfg.NewCluster:						//没有WAL日志文件且当前集群是新建的
		if err = cfg.VerifyBootstrap(); err != nil {		//对当前节点启动使用的配置进行一系列检测
			return nil, err
		}
		cl, err = membership.NewClusterFromURLsMap(cfg.InitialClusterToken, cfg.InitialPeerURLsMap)		//根据配置信息创建本地RaftCluster实例
		if err != nil {
			return nil, err
		}
		m := cl.MemberByName(cfg.Name)						//根据当前节点的名称，从RaftCluster中查找当前节点对应的Member实例
		//从集群中其他节点获取当前集群的信息，检测是否有同名的节点已经启动了
		if isMemberBootstrapped(cl, cfg.Name, prt, cfg.bootstrapTimeout()) {
			return nil, fmt.Errorf("member %s has already been bootstrapped", m.ID)
		}
		if cfg.ShouldDiscover() {							//根据当前的配置检测是否需要使用Discover模式启动
			var str string
			str, err = discovery.JoinCluster(cfg.DiscoveryURL, cfg.DiscoveryProxy, m.ID, cfg.InitialPeerURLsMap.String())
			if err != nil {
				return nil, &DiscoveryError{Op: "join", Err: err}
			}
			var urlsmap types.URLsMap
			urlsmap, err = types.NewURLsMap(str)
			if err != nil {
				return nil, err
			}
			if checkDuplicateURL(urlsmap) {
				return nil, fmt.Errorf("discovery cluster %s has duplicate url", urlsmap)
			}
			if cl, err = membership.NewClusterFromURLsMap(cfg.InitialClusterToken, urlsmap); err != nil {
				return nil, err
			}
		}
		cl.SetStore(st)								//设置本地RaftCluster实例中的store字段
		cl.SetBackend(be)							//设置本地RaftCluster实例中的be字段，同时会在BoltDB中初始化后续使用的Bucket
		cfg.PrintWithInitial()
		id, n, s, w = startNode(cfg, cl, cl.MemberIDs())				//调用startNode函数
	//场景3：wal目录下存在的WAL日志文件的场景
	case haveWAL:									//存在WAL日志文件
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {	//检测member和wal文件夹都是可写的
			return nil, fmt.Errorf("cannot write to member directory: %v", err)
		}

		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil {
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}

		if cfg.ShouldDiscover() {
			plog.Warningf("discovery token ignored since a cluster has already been initialized. Valid log found at %q", cfg.WALDir())
		}
		snapshot, err = ss.Load()										//加载快照数据
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}
		if snapshot != nil {											//根据加载的快照数据，对V2存储和V3存储进行恢复
			if err = st.Recovery(snapshot.Data); err != nil {						//使用快照数据恢复V2存储
				plog.Panicf("recovered store from snapshot error: %v", err)
			}
			plog.Infof("recovered store from snapshot at index %d", snapshot.Metadata.Index)
			if be, err = recoverSnapshotBackend(cfg, be, *snapshot); err != nil {	//使用快照数据恢复V3存储
				plog.Panicf("recovering backend from snapshot error: %v", err)
			}
		}
		cfg.Print()
		if !cfg.ForceNewCluster {
			id, cl, n, s, w = restartNode(cfg, snapshot)							//重启raft.Node节点
		} else {																	//启动单节点的方式
			id, cl, n, s, w = restartAsStandaloneNode(cfg, snapshot)
		}
		cl.SetStore(st)						//设置本地RaftCluster实例的相关字段
		cl.SetBackend(be)
		cl.Recover(api.UpdateCapability)	//从V2存储中，恢复集群中其他节点的信息，其中还会检测当前服务端的版本与WAL日志文件以及快照数据的版本直接的兼容性
		if cl.Version() != nil && !cl.Version().LessThan(semver.Version{Major: 3}) && !beExist {
			os.RemoveAll(bepath)
			return nil, fmt.Errorf("database file (%v) of the backend is missing", bepath)
		}
		//该场景主要是通过快照数据恢复当前节点的V2和V3存储，然后恢复etcd-raft模块中的Node实例。其中V2存储就是假装快照文件中的JSON数据。V3存储的恢复是在
		//recoverSnapshotBackend()函数实现的。
	default:								//不支持其他场景
		return nil, fmt.Errorf("unsupported bootstrap config")
	}

	if terr := fileutil.TouchDirAll(cfg.MemberDir()); terr != nil {
		return nil, fmt.Errorf("cannot access member directory: %v", terr)
	}

	sstats := stats.NewServerStats(cfg.Name, id.String())
	lstats := stats.NewLeaderStats(id.String())

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
	srv = &EtcdServer{										//创建EtcdServer实例
		readych:     make(chan struct{}),
		Cfg:         cfg,
		errorc:      make(chan error, 1),
		store:       st,
		snapshotter: ss,
		r: *newRaftNode(									//使用前面创建的各个组件创建etcdserver.raftNode实例
			raftNodeConfig{
				isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
				Node:        n,
				heartbeat:   heartbeat,
				raftStorage: s,
				storage:     NewStorage(w, ss),
			},
		),
		id:            id,
		attributes:    membership.Attributes{Name: cfg.Name, ClientURLs: cfg.ClientURLs.StringSlice()},
		cluster:       cl,
		stats:         sstats,
		lstats:        lstats,
		SyncTicker:    time.NewTicker(500 * time.Millisecond),
		peerRt:        prt,
		reqIDGen:      idutil.NewGenerator(uint16(id), time.Now()),
		forceVersionC: make(chan struct{}),
	}
	serverID.With(prometheus.Labels{"server_id": id.String()}).Set(1)

	srv.applyV2 = &applierV2store{store: srv.store, cluster: srv.cluster}			//初始化EtcdServer.applyV2字段

	srv.be = be																		//初始化EtcdServer.be字段
	minTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases.	因为在store.restore()方法中除了恢复内存索引，还会重新绑定键值对与
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.			对应的Lease，所以需要先恢复EtcdServer.lessor，再恢复EtcdServer.kv字段。
	srv.lessor = lease.NewLessor(srv.be, int64(math.Ceil(minTTL.Seconds())))
	srv.kv = mvcc.New(srv.be, srv.lessor, &srv.consistIndex)
	if beExist {
		kvindex := srv.kv.ConsistentIndex()
		// TODO: remove kvindex != 0 checking when we do not expect users to upgrade
		// etcd from pre-3.0 release.
		if snapshot != nil && kvindex < snapshot.Metadata.Index {
			if kvindex != 0 {
				return nil, fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d).", bepath, kvindex, snapshot.Metadata.Index)
			}
			plog.Warningf("consistent index never saved (snapshot index=%d)", snapshot.Metadata.Index)
		}
	}
	newSrv := srv // since srv == nil in defer if srv is returned as nil
	defer func() {
		// closing backend without first closing kv can cause
		// resumed compactions to fail with closed tx errors
		if err != nil {
			newSrv.kv.Close()
		}
	}()

	srv.consistIndex.setConsistentIndex(srv.kv.ConsistentIndex())			//根据ConsistentIndex检测当前的Backend实例是否可用
	tp, err := auth.NewTokenProvider(cfg.AuthToken,
		func(index uint64) <-chan struct{} {
			return srv.applyWait.Wait(index)
		},
	)
	if err != nil {
		plog.Errorf("failed to create token provider: %s", err)
		return nil, err
	}
	srv.authStore = auth.NewAuthStore(srv.be, tp)							//初始化EtcdServer.authStore字段
	if num := cfg.AutoCompactionRetention; num != 0 {						//启动后台goroutine，进行自动压缩
		srv.compactor, err = compactor.New(cfg.AutoCompactionMode, num, srv.kv, srv)
		if err != nil {
			return nil, err
		}
		srv.compactor.Run()
	}

	srv.applyV3Base = srv.newApplierV3Backend()								//初始化applyV3Base字段
	if err = srv.restoreAlarms(); err != nil {								//初始化alarmStore字段以及applyV3字段
		return nil, err
	}

	// TODO: move transport initialization near the definition of remote
	tr := &rafthttp.Transport{												//创建rafthttp.Transport实例
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.peerDialTimeout(),
		ID:          id,
		URLs:        cfg.PeerURLs,
		ClusterID:   cl.ID(),
		Raft:        srv,
		Snapshotter: ss,
		ServerStats: sstats,
		LeaderStats: lstats,
		ErrorC:      srv.errorc,
	}
	if err = tr.Start(); err != nil {										//启动rafthttp.Transport实例
		return nil, err
	}
	// add all remotes into transport										向rafthttp.Transport实例中添加集群中各个节点对应的Peer实例和Remote实例
	for _, m := range remotes {
		if m.ID != id {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range cl.Members() {
		if m.ID != id {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	srv.r.transport = tr				//设置raft.Node.transport字段

	return srv, nil
}

func (s *EtcdServer) adjustTicks() {
	clusterN := len(s.cluster.Members())

	// single-node fresh start, or single-node recovers from snapshot
	if clusterN == 1 {
		ticks := s.Cfg.ElectionTicks - 1
		plog.Infof("%s as single-node; fast-forwarding %d ticks (election ticks %d)", s.ID(), ticks, s.Cfg.ElectionTicks)
		s.r.advanceTicks(ticks)
		return
	}

	if !s.Cfg.InitialElectionTickAdvance {
		plog.Infof("skipping initial election tick advance (election tick %d)", s.Cfg.ElectionTicks)
		return
	}

	// retry up to "rafthttp.ConnReadTimeout", which is 5-sec
	// until peer connection reports; otherwise:
	// 1. all connections failed, or
	// 2. no active peers, or
	// 3. restarted single-node with no snapshot
	// then, do nothing, because advancing ticks would have no effect
	waitTime := rafthttp.ConnReadTimeout
	itv := 50 * time.Millisecond
	for i := int64(0); i < int64(waitTime/itv); i++ {
		select {
		case <-time.After(itv):
		case <-s.stopping:
			return
		}

		peerN := s.r.transport.ActivePeers()
		if peerN > 1 {
			// multi-node received peer connection reports
			// adjust ticks, in case slow leader message receive
			ticks := s.Cfg.ElectionTicks - 2
			plog.Infof("%s initialzed peer connection; fast-forwarding %d ticks (election ticks %d) with %d active peer(s)", s.ID(), ticks, s.Cfg.ElectionTicks, peerN)
			s.r.advanceTicks(ticks)
			return
		}
	}
}

// Start performs any initialization of the Server necessary for it to
// begin serving requests. It must be called before Do or Process.
// Start must be non-blocking; any long-running server functionality
// should be implemented in goroutines.
func (s *EtcdServer) Start() {				//启动当前节点
	s.start()													//其中会启动一个后台goroutine，执行EtcdServer.run()方法
	s.goAttach(func() { s.adjustTicks() })
	s.goAttach(func() { s.publish(s.Cfg.ReqTimeout()) })		//启动一个后台goroutine，将当前节点的相关信息发送到集群其他节点
	s.goAttach(s.purgeFile)										//启动一个后台goroutine，定义清理WAL日志文件和快照文件
	s.goAttach(func() { monitorFileDescriptor(s.stopping) })	//启动一个后台goroutine，实现一些监控相关功能
	s.goAttach(s.monitorVersions)								//启动一个后台goroutine监控集群中其他节点的版本信息，主要是在版本升级的时候使用
	s.goAttach(s.linearizableReadLoop)							//启动一个后台goroutine，用来实现Linearizable Read的功能
	s.goAttach(s.monitorKVHash)
}

// start prepares and starts server in a new goroutine. It is no longer safe to		该方法会初始化EtcdServer实例中剩余的未初始化字段，然后启动后台goroutine来
// modify a server's fields after it has been sent to Start.                        执行EtcdServer.run()方法。run()方法是EtcdServer启动的核心，其中会启动前面
// This function is just used for testing.                                          EtcdServer.raftNode实例，然后出了etcd-raft模块返回的Ready实例。
func (s *EtcdServer) start() {
	if s.Cfg.SnapCount == 0 {
		plog.Infof("set snapshot count to default %d", DefaultSnapCount)
		s.Cfg.SnapCount = DefaultSnapCount
	}
	s.w = wait.New()
	s.applyWait = wait.NewTimeList()
	s.done = make(chan struct{})
	s.stop = make(chan struct{})
	s.stopping = make(chan struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.readwaitc = make(chan struct{}, 1)
	s.readNotifier = newNotifier()
	if s.ClusterVersion() != nil {
		plog.Infof("starting server... [version: %v, cluster version: %v]", version.Version, version.Cluster(s.ClusterVersion().String()))
	} else {
		plog.Infof("starting server... [version: %v, cluster version: to_be_decided]", version.Version)
	}
	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go s.run()
}
//该方法会启动两个后台goroutine，其中一个后台goroutine负责定期清理WAL日志文件，另一个后台goroutine负责定期清理快照文件。
func (s *EtcdServer) purgeFile() {
	var dberrc, serrc, werrc <-chan error
	if s.Cfg.MaxSnapFiles > 0 {
		dberrc = fileutil.PurgeFile(s.Cfg.SnapDir(), "snap.db", s.Cfg.MaxSnapFiles, purgeFileInterval, s.done)
		//这里会启动一个后台goroutine，定期清理快照文件(默认purgeFileInterval的值为30s)
		serrc = fileutil.PurgeFile(s.Cfg.SnapDir(), "snap", s.Cfg.MaxSnapFiles, purgeFileInterval, s.done)
	}
	if s.Cfg.MaxWALFiles > 0 {
		//启动一个后台goroutine，定期清理WAL日志文件(默认purgeFileInterval的值为30s)
		werrc = fileutil.PurgeFile(s.Cfg.WALDir(), "wal", s.Cfg.MaxWALFiles, purgeFileInterval, s.done)
	}
	select {
	case e := <-dberrc:
		plog.Fatalf("failed to purge snap db file %v", e)
	case e := <-serrc:
		plog.Fatalf("failed to purge snap file %v", e)
	case e := <-werrc:
		plog.Fatalf("failed to purge wal file %v", e)
	case <-s.stopping:
		return
	}
}

func (s *EtcdServer) ID() types.ID { return s.id }

func (s *EtcdServer) Cluster() api.Cluster { return s.cluster }

func (s *EtcdServer) ApplyWait() <-chan struct{} { return s.applyWait.Wait(s.getCommittedIndex()) }

type ServerPeer interface {
	ServerV2
	RaftHandler() http.Handler
	LeaseHandler() http.Handler
}

func (s *EtcdServer) LeaseHandler() http.Handler {
	if s.lessor == nil {
		return nil
	}
	return leasehttp.NewHandler(s.lessor, s.ApplyWait)
}

func (s *EtcdServer) RaftHandler() http.Handler { return s.r.transport.Handler() }

// Process takes a raft message and applies it to the server's raft state
// machine, respecting any timeout of the given context.
func (s *EtcdServer) Process(ctx context.Context, m raftpb.Message) error {
	if s.cluster.IsIDRemoved(types.ID(m.From)) {
		plog.Warningf("reject message from removed member %s", types.ID(m.From).String())
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	if m.Type == raftpb.MsgApp {
		s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	}
	return s.r.Step(ctx, m)
}

func (s *EtcdServer) IsIDRemoved(id uint64) bool { return s.cluster.IsIDRemoved(types.ID(id)) }

func (s *EtcdServer) ReportUnreachable(id uint64) { s.r.ReportUnreachable(id) }

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (s *EtcdServer) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	s.r.ReportSnapshot(id, status)
}

type etcdProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}
// 该结构体功能：在结构体EtcdServer中记录了当前节点的状态信息，例如，当前是否是Leader节点、Entry记录的提交位置等。
// raftReadyHandler contains a set of EtcdServer operations to be called by raftNode,
// and helps decouple state machine logic from Raft algorithms.
// TODO: add a state machine interface to apply the commit entries and do snapshot/recover
type raftReadyHandler struct {
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(uint64)
}

func (s *EtcdServer) run() {
	sn, err := s.r.raftStorage.Snapshot()
	if err != nil {
		plog.Panicf("get snapshot from raft storage error: %v", err)
	}

	// asynchronously accept apply packets, dispatch progress in-order
	sched := schedule.NewFIFOScheduler()					//FIFO调度器

	var (
		smu   sync.RWMutex
		syncC <-chan time.Time
	)
	setSyncC := func(ch <-chan time.Time) {				//setSyncC()和getSyncC()方法是用来设置发送SYNC消息的定时器
		smu.Lock()
		syncC = ch
		smu.Unlock()
	}
	getSyncC := func() (ch <-chan time.Time) {
		smu.RLock()
		ch = syncC
		smu.RUnlock()
		return
	}
	//raftNode在处理etcd-raft模块返回的Ready.SoftState字段时，会调用raftReadyHandler.updateLeadership()回调函数，其中会根据当前节点的状态和Leader节点是否
	//发生变化完成一些相应的操作。
	rh := &raftReadyHandler{
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() {
				if s.lessor != nil {
					s.lessor.Demote()				//调用lessor.Demote()将节点的Lessor实例降级
				}
				if s.compactor != nil {				//非Leader节点暂停自动压缩
					s.compactor.Pause()
				}
				setSyncC(nil)					//非Leader节点不会发送SYNC消息，将该定时器设置为nil
			} else {
				if newLeader {
					t := time.Now()
					s.leadTimeMu.Lock()
					//如果发生Leader节点的切换，且当前节点成为Leader节点，则初始化leadElectedTime字段，该字段记录了当前节点最近一次成为Leader节点的时间
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				setSyncC(s.SyncTicker.C)			//Leader节点会定期发送SYNC消息，恢复该定时器
				if s.compactor != nil {				//重启自动压缩的功能
					s.compactor.Resume()
				}
			}

			// TODO: remove the nil checking
			// current test utility does not provide the stats
			if s.stats != nil {
				s.stats.BecomeLeader()
			}
		},
		//在raftNode处理apply实例时会调用updateCommittedIndex()函数，该函数会根据apply实例中封装的待应用Entry记录和快照数据确定当前的committedIndex值，然后
		//调用raftReadyHandler中的同名回调函数更新EtcdServer.committedIndex字段值
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	//启动raftNode，其中会启动后台goroutine处理etcd-raft模块返回的Ready实例
	s.r.start(rh)
	//记录当前快照相关的元数据信息和已应用Entry记录的位置信息
	ep := etcdProgress{
		confState: sn.Metadata.ConfState,
		snapi:     sn.Metadata.Index,
		appliedt:  sn.Metadata.Term,
		appliedi:  sn.Metadata.Index,
	}

	defer func() {
		s.wgMu.Lock() // block concurrent waitgroup adds in goAttach while stopping
		close(s.stopping)
		s.wgMu.Unlock()
		s.cancel()

		sched.Stop()

		// wait for gouroutines before closing raft so wal stays open
		s.wg.Wait()

		s.SyncTicker.Stop()

		// must stop raft after scheduler-- etcdserver can leak rafthttp pipelines
		// by adding a peer after raft stops the transport
		s.r.stop()

		// kv, lessor and backend can be nil if running without v3 enabled
		// or running unit tests.
		if s.lessor != nil {
			s.lessor.Stop()
		}
		if s.kv != nil {
			s.kv.Close()
		}
		if s.authStore != nil {
			s.authStore.Close()
		}
		if s.be != nil {
			s.be.Close()
		}
		if s.compactor != nil {
			s.compactor.Stop()
		}
		close(s.done)
	}()

	var expiredLeaseC <-chan []*lease.Lease
	if s.lessor != nil {
		expiredLeaseC = s.lessor.ExpiredLeasesC()
	}

	for {
		select {
		case ap := <-s.r.apply():			//读取raftNode.applyc通道中的apply实例并进行处理
			f := func(context.Context) { s.applyAll(&ep, &ap) }
			sched.Schedule(f)
		case leases := <-expiredLeaseC:		//监听expiredLeaseC通道
			s.goAttach(func() {				//启动单独的goroutine
				// Increases throughput of expired leases deletion process through parallelization
				c := make(chan struct{}, maxPendingRevokes)				//用于限流，上限是16
				for _, lease := range leases {								//遍历过期的Lease实例
					select {
					case c <- struct{}{}:                                 //向c通道中添加一个空结构体
					case <-s.stopping:
						return
					}
					lid := lease.ID
					s.goAttach(func() {										//启动一个后台线程，完成指定Lease的撤销
						ctx := s.authStore.WithRoot(s.ctx)
						_, lerr := s.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: int64(lid)})
						if lerr == nil {
							leaseExpired.Inc()
						} else {
							plog.Warningf("failed to revoke %016x (%q)", lid, lerr.Error())
						}

						<-c
					})
				}
			})
		case err := <-s.errorc:
			plog.Errorf("%s", err)
			plog.Infof("the data-dir used by this member must be removed.")
			return
		case <-getSyncC():						//定时发送SYNC消息
			if s.store.HasTTLKeys() {			//如果V2存储中只有永久节点，则无须发送SYNC
				s.sync(s.Cfg.ReqTimeout())		//发送SYNC消息的目的是为了清理V2存储中的过期节点
			}
		case <-s.stop:
			return
		}
	}
}
//该方法首先调用applySnapshot()方法处理apply实例中记录的快照数据，然后调用applyEntries方法处理apply实例中的Entry记录，之后根据apply实例的处理结果检测是否需要
//生成新的快照文件，最后处理MsgSnap消息。
func (s *EtcdServer) applyAll(ep *etcdProgress, apply *apply) {
	s.applySnapshot(ep, apply)						//调用applySnapshot方法处理apply实例中记录的快照数据
	s.applyEntries(ep, apply)						//调用applyEntries方法处理apply实例中的Entry记录

	proposalsApplied.Set(float64(ep.appliedi))
	//etcdProgress.appliedi记录了已应用Entry的索引值。这里通过调用WaitTime.Trigger()方法将id小于endProgress.appliedi的Entry对应的通道全部关闭，这样可以通知
	//其他监听通道的goroutine。
	s.applyWait.Trigger(ep.appliedi)
	// wait for the raft routine to finish the disk writes before triggering a	当Ready处理基本完成时，会向notifyc通道中写入一个信号，通知当前goroutine去检测
	// snapshot. or applied index might be greater than the last index in raft	是否需要生成快照。
	// storage, since the raft routine might be slower than apply routine.
	<-apply.notifyc

	s.triggerSnapshot(ep)														//根据当前状态决定是否触发快照的生成
	select {
	// snapshot requested via send()
	// 在raftNode中处理Ready实例时，如果并没有直接发送MsgSnap消息，而是将其写入msgSnapC通道中，这里会读取MsgSnapC通道，并完成快照数据的发送。
	case m := <-s.r.msgSnapC:
		//将V2存储的快照数据和V3存储的数据合并成完整的快照数据
		merged := s.createMergedSnapshotMessage(m, ep.appliedt, ep.appliedi, ep.confState)
		s.sendMergedSnap(merged)												//发送快照数据
	default:
	}
}
//该方法会先等待raftNode将快照数据持久化到磁盘中，之后根据快照元数据查找BoltDB数据库文件并重建Backend实例，最后根据重建后的存储更新本地RaftCluster实例。
func (s *EtcdServer) applySnapshot(ep *etcdProgress, apply *apply) {
	if raft.IsEmptySnap(apply.snapshot) {							//检测待应用的快照数据是否为空，如果为空则直接返回
		return
	}

	plog.Infof("applying snapshot at index %d...", ep.snapi)
	defer plog.Infof("finished applying incoming snapshot at index %d", ep.snapi)

	if apply.snapshot.Metadata.Index <= ep.appliedi {				//如果该快照中最后一条Entry的索引值小于当前节点已应用Entry索引值，则程序异常结束
		plog.Panicf("snapshot index [%d] should > appliedi[%d] + 1",
			apply.snapshot.Metadata.Index, ep.appliedi)
	}

	// wait for raftNode to persist snapshot onto the disk
	// raftNode在将快照数据写入磁盘文件之后，会向notifc通道中写入一个空结构体作为信号，这里会阻塞等待该信号
	<-apply.notifyc
	//根据快照信息查找对应的BoltDB数据库文件，并创建新的Backend实例
	newbe, err := openSnapshotBackend(s.Cfg, s.snapshotter, apply.snapshot)
	if err != nil {
		plog.Panic(err)
	}

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases. 因为在store.restore()方法中除了恢复内存索引，还会重新绑定
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.      键值对与对应的Lease，所以需要先恢复EtcdServer.lessor,再
	if s.lessor != nil {																				 //恢复EtcdServer.kv字段
		plog.Info("recovering lessor...")
		s.lessor.Recover(newbe, func() lease.TxnDelete { return s.kv.Write() })
		plog.Info("finished recovering lessor")
	}

	plog.Info("restoring mvcc store...")

	if err := s.kv.Restore(newbe); err != nil {					//重置EtcdServer.consistIndex字段
		plog.Panicf("restore KV error: %v", err)
	}
	s.consistIndex.setConsistentIndex(s.kv.ConsistentIndex())

	plog.Info("finished restoring mvcc store")

	// Closing old backend might block until all the txns
	// on the backend are finished.
	// We do not want to wait on closing the old backend.
	s.bemu.Lock()
	oldbe := s.be
	go func() {
		plog.Info("closing old backend...")
		defer plog.Info("finished closing old backend")
		//因为此时可能还有事务在执行，关闭旧Backend实例可能会被阻塞，所以这里启动一个后台goroutine用来关闭Backend实例
		if err := oldbe.Close(); err != nil {
			plog.Panicf("close backend error: %v", err)
		}
	}()

	s.be = newbe			//更新EtcdServer实例中使用的Backend实例
	s.bemu.Unlock()

	plog.Info("recovering alarms...")
	//恢复EtcdServer中的alarmStore和authStore，它们分别对应BoltDB中的alarm Bucket和auth Bucket
	if err := s.restoreAlarms(); err != nil {
		plog.Panicf("restore alarms error: %v", err)
	}
	plog.Info("finished recovering alarms")

	if s.authStore != nil {
		plog.Info("recovering auth store...")
		s.authStore.Recover(newbe)
		plog.Info("finished recovering auth store")
	}

	plog.Info("recovering store v2...")
	if err := s.store.Recovery(apply.snapshot.Data); err != nil {			//恢复V2版本存储
		plog.Panicf("recovery store error: %v", err)
	}
	plog.Info("finished recovering store v2")

	s.cluster.SetBackend(s.be)												//设置RaftCluster.backend实例
	plog.Info("recovering cluster configuration...")
	s.cluster.Recover(api.UpdateCapability)									//恢复本地的集群信息
	plog.Info("finished recovering cluster configuration")

	plog.Info("removing old peers from network...")
	// recover raft transport
	s.r.transport.RemoveAllPeers()											//清空Transport中所有Peer实例，并根据恢复后的RaftCluster实例重新添加
	plog.Info("finished removing old peers from network")

	plog.Info("adding peers from new cluster configuration into network...")
	for _, m := range s.cluster.Members() {
		if m.ID == s.ID() {
			continue
		}
		s.r.transport.AddPeer(m.ID, m.PeerURLs)
	}
	plog.Info("finished adding peers from new cluster configuration into network...")
	//更新etcdProgress，其中涉及已应用Entry记录的Term值、Index值和快照相关信息
	ep.appliedt = apply.snapshot.Metadata.Term
	ep.appliedi = apply.snapshot.Metadata.Index
	ep.snapi = ep.appliedi
	ep.confState = apply.snapshot.Metadata.ConfState
}
//应用完快照数据之后，run goroutine紧接着会调用EtcdServer.applyEntries方法处理待应用的Entry记录。
func (s *EtcdServer) applyEntries(ep *etcdProgress, apply *apply) {
	if len(apply.entries) == 0 {
		return
	}
	firsti := apply.entries[0].Index						//检测是否存在待应用的Entry记录，如果为空则直接返回
	if firsti > ep.appliedi+1 {								//检测待应用的第一条Entry记录是否合法
		plog.Panicf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, ep.appliedi)
	}
	var ents []raftpb.Entry
	if ep.appliedi+1-firsti < uint64(len(apply.entries)) {
		ents = apply.entries[ep.appliedi+1-firsti:]			//忽略已应用的Entry记录，只保留未应用的Entry记录
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if ep.appliedt, ep.appliedi, shouldstop = s.apply(ents, &ep.confState); shouldstop {		//调用apply()方法应用ents中的Entry记录
		go s.stopWithDelay(10*100*time.Millisecond, fmt.Errorf("the member has been permanently removed from the cluster"))
	}
}
//该方法对是否需要生成新快照文件的判定
func (s *EtcdServer) triggerSnapshot(ep *etcdProgress) {
	if ep.appliedi-ep.snapi <= s.Cfg.SnapCount {
		return								//连续应用一定量的Entry记录，会触发快照的生成(snapCount默认为100000条)
	}

	plog.Infof("start to snapshot (applied: %d, lastsnap: %d)", ep.appliedi, ep.snapi)
	s.snapshot(ep.appliedi, ep.confState)	//创建新的快照文件
	ep.snapi = ep.appliedi					//更新etcdProgress.snapi
}

func (s *EtcdServer) isMultiNode() bool {
	return s.cluster != nil && len(s.cluster.MemberIDs()) > 1
}

func (s *EtcdServer) isLeader() bool {
	return uint64(s.ID()) == s.Lead()
}

// MoveLeader transfers the leader to the given transferee.
func (s *EtcdServer) MoveLeader(ctx context.Context, lead, transferee uint64) error {
	now := time.Now()
	interval := time.Duration(s.Cfg.TickMs) * time.Millisecond

	plog.Infof("%s starts leadership transfer from %s to %s", s.ID(), types.ID(lead), types.ID(transferee))
	s.r.TransferLeadership(ctx, lead, transferee)
	for s.Lead() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(interval):
		}
	}

	// TODO: drain all requests, or drop all messages to the old leader

	plog.Infof("%s finished leadership transfer from %s to %s (took %v)", s.ID(), types.ID(lead), types.ID(transferee), time.Since(now))
	return nil
}

// TransferLeadership transfers the leader to the chosen transferee.
func (s *EtcdServer) TransferLeadership() error {
	if !s.isLeader() {
		plog.Printf("skipped leadership transfer for stopping non-leader member")
		return nil
	}

	if !s.isMultiNode() {
		plog.Printf("skipped leadership transfer for single member cluster")
		return nil
	}

	transferee, ok := longestConnected(s.r.transport, s.cluster.MemberIDs())
	if !ok {
		return ErrUnhealthy
	}

	tm := s.Cfg.ReqTimeout()
	ctx, cancel := context.WithTimeout(s.ctx, tm)
	err := s.MoveLeader(ctx, s.Lead(), uint64(transferee))
	cancel()
	return err
}

// HardStop stops the server without coordination with other members in the cluster.
func (s *EtcdServer) HardStop() {
	select {
	case s.stop <- struct{}{}:
	case <-s.done:
		return
	}
	<-s.done
}

// Stop stops the server gracefully, and shuts down the running goroutine.
// Stop should be called after a Start(s), otherwise it will block forever.
// When stopping leader, Stop transfers its leadership to one of its peers
// before stopping the server.
// Stop terminates the Server and performs any necessary finalization.
// Do and Process cannot be called after Stop has been invoked.
func (s *EtcdServer) Stop() {
	if err := s.TransferLeadership(); err != nil {
		plog.Warningf("%s failed to transfer leadership (%v)", s.ID(), err)
	}
	s.HardStop()
}

// ReadyNotify returns a channel that will be closed when the server
// is ready to serve client requests
func (s *EtcdServer) ReadyNotify() <-chan struct{} { return s.readych }

func (s *EtcdServer) stopWithDelay(d time.Duration, err error) {
	select {
	case <-time.After(d):
	case <-s.done:
	}
	select {
	case s.errorc <- err:
	default:
	}
}

// StopNotify returns a channel that receives a empty struct
// when the server is stopped.
func (s *EtcdServer) StopNotify() <-chan struct{} { return s.done }

func (s *EtcdServer) SelfStats() []byte { return s.stats.JSON() }

func (s *EtcdServer) LeaderStats() []byte {
	lead := atomic.LoadUint64(&s.r.lead)
	if lead != uint64(s.id) {
		return nil
	}
	return s.lstats.JSON()
}

func (s *EtcdServer) StoreStats() []byte { return s.store.JsonStats() }

func (s *EtcdServer) checkMembershipOperationPermission(ctx context.Context) error {
	if s.authStore == nil {
		// In the context of ordinary etcd process, s.authStore will never be nil.
		// This branch is for handling cases in server_test.go
		return nil
	}

	// Note that this permission check is done in the API layer,
	// so TOCTOU problem can be caused potentially in a schedule like this:
	// update membership with user A -> revoke root role of A -> apply membership change
	// in the state machine layer
	// However, both of membership change and role management requires the root privilege.
	// So careful operation by admins can prevent the problem.
	authInfo, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}

	return s.AuthStore().IsAdminPermitted(authInfo)
}

func (s *EtcdServer) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	if s.Cfg.StrictReconfigCheck {
		// by default StrictReconfigCheck is enabled; reject new members if unhealthy
		if !s.cluster.IsReadyToAddNewMember() {
			plog.Warningf("not enough started members, rejecting member add %+v", memb)
			return nil, ErrNotEnoughStartedMembers
		}
		if !isConnectedFullySince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), s.cluster.Members()) {
			plog.Warningf("not healthy for reconfigure, rejecting member add %+v", memb)
			return nil, ErrUnhealthy
		}
	}

	// TODO: move Member to protobuf type
	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return s.configure(ctx, cc)
}

func (s *EtcdServer) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	// by default StrictReconfigCheck is enabled; reject removal if leads to quorum loss
	if err := s.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return s.configure(ctx, cc)
}

func (s *EtcdServer) mayRemoveMember(id types.ID) error {
	if !s.Cfg.StrictReconfigCheck {
		return nil
	}

	if !s.cluster.IsReadyToRemoveMember(uint64(id)) {
		plog.Warningf("not enough started members, rejecting remove member %s", id)
		return ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := s.r.transport.ActiveSince(id); id != s.ID() && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := s.cluster.Members()
	active := numConnectedSince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		plog.Warningf("reconfigure breaks active quorum, rejecting remove member %s", id)
		return ErrUnhealthy
	}

	return nil
}

func (s *EtcdServer) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return s.configure(ctx, cc)
}

// Implement the RaftTimer interface

func (s *EtcdServer) Index() uint64 { return atomic.LoadUint64(&s.r.index) }

func (s *EtcdServer) Term() uint64 { return atomic.LoadUint64(&s.r.term) }

// Lead is only for testing purposes.
// TODO: add Raft server interface to expose raft related info:
// Index, Term, Lead, Committed, Applied, LastIndex, etc.
func (s *EtcdServer) Lead() uint64 { return atomic.LoadUint64(&s.r.lead) }

func (s *EtcdServer) Leader() types.ID { return types.ID(s.Lead()) }

type confChangeResponse struct {
	membs []*membership.Member
	err   error
}

// configure sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (s *EtcdServer) configure(ctx context.Context, cc raftpb.ConfChange) ([]*membership.Member, error) {
	cc.ID = s.reqIDGen.Next()
	ch := s.w.Register(cc.ID)
	start := time.Now()
	if err := s.r.ProposeConfChange(ctx, cc); err != nil {
		s.w.Trigger(cc.ID, nil)
		return nil, err
	}
	select {
	case x := <-ch:
		if x == nil {
			plog.Panicf("configure trigger value should never be nil")
		}
		resp := x.(*confChangeResponse)
		return resp.membs, resp.err
	case <-ctx.Done():
		s.w.Trigger(cc.ID, nil) // GC wait
		return nil, s.parseProposeCtxErr(ctx.Err(), start)
	case <-s.stopping:
		return nil, ErrStopped
	}
}
// 在run goroutine中通过getSyncC()函数监听该定时器，当定时器到期时会调用EtcdServer.sync()方法发送SYNC消息。
// sync proposes a SYNC request and is non-blocking.						SYNC消息的主要作用是定义通知节点，清理过期V2存储中的过期节点。再raftReadyHandler时
// This makes no guarantee that the request will be proposed or performed.	提到，当节点成为Leader时，会调用setSyncC()回调函数设置一个定时器，用来触发SYNC消息
// The request will be canceled after the given timeout.					的定期发送。
func (s *EtcdServer) sync(timeout time.Duration) {
	req := pb.Request{														//创建SYNC消息
		Method: "SYNC",
		ID:     s.reqIDGen.Next(),
		Time:   time.Now().UnixNano(),
	}
	data := pbutil.MustMarshal(&req)										//序列化SYNC消息
	// There is no promise that node has leader when do SYNC request,
	// so it uses goroutine to propose.
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	s.goAttach(func() {														//启动一个后台goroutine，发送SYNC(MsgProp)消息
		s.r.Propose(ctx, data)
		cancel()
	})
}
// 该方法将当前节点的相关信息发送到集群其他节点(即将当前节点注册到集群当中)
// publish registers server information into the cluster. The information
// is the JSON representation of this server's member struct, updated with the
// static clientURLs of the server.
// The function keeps attempting to register until it succeeds,
// or its server is stopped.
func (s *EtcdServer) publish(timeout time.Duration) {
	b, err := json.Marshal(s.attributes)					//将EtcdServer.attributes字段序列化成JSON格式
	if err != nil {
		plog.Panicf("json marshal error: %v", err)
		return
	}
	req := pb.Request{										//将上述JSON数据封装成PUT请求
		Method: "PUT",
		Path:   membership.MemberAttributesStorePath(s.id),
		Val:    string(b),
	}

	for {
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		_, err := s.Do(ctx, req)							//调用EtcdServer.Do()方法处理该请求
		cancel()
		switch err {
		case nil:
			//将当前节点信息发送到集群其他节点之后，会将readych通道关闭，从而实现通知其他goroutine的目的
			close(s.readych)
			plog.Infof("published %+v to cluster %s", s.attributes, s.cluster.ID())
			return
		case ErrStopped:
			//如果出现错误，则会输出相应的日志，然后继续当前的for循环，直至注册成功
			plog.Infof("aborting publish because server is stopped")
			return
		default:
			plog.Errorf("publish error: %v", err)
		}
	}
}
//创建完snap.Message实例之后会调用该方法将其发送到指定节点。
func (s *EtcdServer) sendMergedSnap(merged snap.Message) {
	atomic.AddInt64(&s.inflightSnapshots, 1)				//递增inflightSnapshots字段，它表示已发送但未收到响应的快照消息个数
	//发送Snap.Message消息，底层会启动单独的后台goroutine，通过snapshotSender完成发送。
	s.r.transport.SendSnapshot(merged)
	s.goAttach(func() {											//启动一个后台goroutine监听该快照消息是否发送完成
		select {
		case ok := <-merged.CloseNotify():
			// delay releasing inflight snapshot for another 30 seconds to
			// block log compaction.
			// If the follower still fails to catch up, it is probably just too slow
			// to catch up. We cannot avoid the snapshot cycle anyway.
			if ok {
				select {
				case <-time.After(releaseDelayAfterSnapshot):	//默认阻塞等待30秒
				case <-s.stopping:
				}
			}
			//snap.Message消息发送完成(或是超时)之后会递减inflightSnapshots值，当inflightSnapshots递减到0时，前面对MemoryStorage的压缩才能执行
			atomic.AddInt64(&s.inflightSnapshots, -1)
		case <-s.stopping:
			return
		}
	})
}

// apply takes entries received from Raft (after it has been committed) and		该方法会遍历ents中的全部Entry记录，并根据Entry的类型进行不同的处理。
// applies them to the current state of the EtcdServer.
// The given entries should not be empty.
func (s *EtcdServer) apply(es []raftpb.Entry, confState *raftpb.ConfState) (appliedt uint64, appliedi uint64, shouldStop bool) {
	for i := range es {											//遍历待应用的Entry记录
		e := es[i]
		switch e.Type {											//根据Entry记录的不同类型，进行不同的处理
		case raftpb.EntryNormal:
			s.applyEntryNormal(&e)
		case raftpb.EntryConfChange:
			// set the consistent index of current executing entry	更新EtcdServer.consistIndex，其中保存了当前节点应用的最后一条Entry记录的索引值
			if e.Index > s.consistIndex.ConsistentIndex() {
				s.consistIndex.setConsistentIndex(e.Index)
			}
			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)						//将Entry.Data反序列化成ConfChange实例
			removedSelf, err := s.applyConfChange(cc, confState)	//调用applyConfChange方法处理ConfChange，注意返回值removedSelf，当它为true时表示将当前节点从集群中移除
			s.setAppliedIndex(e.Index)								//更新EtcdServer.appliedIndex字段
			shouldStop = shouldStop || removedSelf
			s.w.Trigger(cc.ID, &confChangeResponse{s.cluster.Members(), err})
		default:
			plog.Panicf("entry type should be either EntryNormal or EntryConfChange")
		}
		atomic.StoreUint64(&s.r.index, e.Index)						//更新raftNode的index字段和term字段返回值
		atomic.StoreUint64(&s.r.term, e.Term)
		appliedt = e.Term											//更新appliedt、appliedi返回值
		appliedi = e.Index
	}
	return appliedt, appliedi, shouldStop							//shouldStop标识当前节点是否已经从集群中移除
}
// 该方法首先会尝试将Entry.Data反序列化成InternalRaftRequest实例，如果失败，则将其反序列化成etcdserverpb.Request实例，之后根据反序列化的结果调用EtcdServer的相应
// 方法进行处理，最后将处理结果写入Entry对应的通道中。
// applyEntryNormal apples an EntryNormal type raftpb request to the EtcdServer
func (s *EtcdServer) applyEntryNormal(e *raftpb.Entry) {
	shouldApplyV3 := false
	if e.Index > s.consistIndex.ConsistentIndex() {
		// set the consistent index of current executing entry
		s.consistIndex.setConsistentIndex(e.Index)					//更新EtcdServer.consistIndex记录的索引值
		shouldApplyV3 = true
	}
	defer s.setAppliedIndex(e.Index)								//方法结束时更新EtcdServer.appliedIndex字段记录的索引值

	// raft state machine may generate noop entry when leader confirmation.
	// skip it in advance to avoid some potential bug in the future
	if len(e.Data) == 0 {											//空的Entry记录只会在Leader选举结束时出现
		select {
		case s.forceVersionC <- struct{}{}:
		default:
		}
		// promote lessor when the local member is leader and finished
		// applying all entries from the last term.
		if s.isLeader() {											//如果当前节点为Leader，则晋升其Lessor实例
			s.lessor.Promote(s.Cfg.electionTimeout())
		}
		return
	}

	var raftReq pb.InternalRaftRequest								//忽略没有数据的Entry记录
	//尝试将Entry.Data反序列化成InternalRaftRequest实例，InternalRaftRequest中封装了所有类型的Client请求
	if !pbutil.MaybeUnmarshal(&raftReq, e.Data) { // backward compatible
		var r pb.Request
		rp := &r
		pbutil.MustUnmarshal(rp, e.Data)							//兼容性处理，如果上述序列化失败，则将Entry.Date反序列化成pb.Request实例
		//调用EtcdServer.applyV2Request()方法进行处理，在applyV2Request方法中，会根据请求的类型，调用不同的方法进行处理。处理结束后会将结果写入Wait中记录
		//的通道中，然后关闭该通道。
		s.w.Trigger(r.ID, s.applyV2Request((*RequestV2)(rp)))
		return
	}
	if raftReq.V2 != nil {											//上述序列化成功，且是V2版本的请求，调用applyV2Request方法处理
		req := (*RequestV2)(raftReq.V2)
		s.w.Trigger(req.ID, s.applyV2Request(req))					//关闭Wait中记录的该Entry对应的通道
		return
	}

	// do not re-apply applied entries.
	if !shouldApplyV3 {
		return
	}
	//下面是对V3版本请求的处理
	id := raftReq.ID									//获取请求id
	if id == 0 {
		id = raftReq.Header.ID
	}

	var ar *applyResult
	needResult := s.w.IsRegistered(id)					//检测该请求是否需要进行响应
	if needResult || !noSideEffect(&raftReq) {
		if !needResult && raftReq.Txn != nil {
			removeNeedlessRangeReqs(raftReq.Txn)
		}
		ar = s.applyV3.Apply(&raftReq)					//调用applyV3.Apply()方法处理该Entry，其中会根据请求的类型选择不同的方法进行处理
	}

	if ar == nil {										//返回结果ar(applyResult类型)为nil，直接返回
		return
	}
	//如果返回了ErrNoSpace错误，则表示底层的Backend已经没有足够的空间，如果是第一次出现这种情况，则在后面立即启动一个后台goroutine，并调用EtcdServer.raftRequest()
	//方法发送AlarmRequest请求，当前其他节点收到该请求时，会停止后续的PUT操作。
	if ar.err != ErrNoSpace || len(s.alarmStore.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.w.Trigger(id, ar)								//将上述处理结果写入对应的通道中，然后将对应通道关闭
		return
	}

	plog.Errorf("applying raft message exceeded backend quota")
	s.goAttach(func() {									//第一次出现ErrNoSpace错误
		a := &pb.AlarmRequest{							//创建AlarmRequest
			MemberID: uint64(s.ID()),
			Action:   pb.AlarmRequest_ACTIVATE,
			Alarm:    pb.AlarmType_NOSPACE,
		}
		s.raftRequest(s.ctx, pb.InternalRaftRequest{Alarm: a})	//将AlarmRequest请求封装成MsgProp消息，并发送到集群中
		s.w.Trigger(id, ar)										//将上述处理结果写入对应的通道中，然后将对应通道关闭
	})
}
// EntryConfChange类型的Entry记录主要是通过EtcdServer.applyConfChange()方法进行处理。该方法会根据ConfChange的类型进行分类处理。
// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func (s *EtcdServer) applyConfChange(cc raftpb.ConfChange, confState *raftpb.ConfState) (bool, error) {
	//在开始进行节点修改之前，先调用ValidateConfigurationChange进行检测
	if err := s.cluster.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = raft.None
		s.r.ApplyConfChange(cc)
		return false, err
	}
	*confState = *s.r.ApplyConfChange(cc)		//将ConfChange交给etcd-raft模块进行处理，其中会根据ConfChange的类型进行分类处理。这里的返回值的ConfState中记录了集群中最新的节点id
	switch cc.Type {							//根据请求的类型进行相应的处理
	case raftpb.ConfChangeAddNode:
		m := new(membership.Member)				//将ConfChange.Context中的数据反序列化成Member实例
		if err := json.Unmarshal(cc.Context, m); err != nil {
			plog.Panicf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			plog.Panicf("nodeID should always be equal to member ID")
		}
		s.cluster.AddMember(m)					//将新Member实例添加到本地RaftCluster中
		if m.ID != s.id {
			//如果添加的是远端节点，则需要在Transport中添加对应的Peer实例
			s.r.transport.AddPeer(m.ID, m.PeerURLs)
		}
	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		s.cluster.RemoveMember(id)				//从RaftCluster中删除指定的Member实例
		if id == s.id {
			return true, nil					//如果移除的是当前节点则返回true
		}
		s.r.transport.RemovePeer(id)			//若移除远端节点，则需要从Transporter中删除对应的Peer实例
	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)				//将ConfChange.Context中的数据反序列化成Member实例
		if err := json.Unmarshal(cc.Context, m); err != nil {
			plog.Panicf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			plog.Panicf("nodeID should always be equal to member ID")
		}
		s.cluster.UpdateRaftAttributes(m.ID, m.RaftAttributes)		//更新本地RaftCluster实例中相应的Member实例
		if m.ID != s.id {						//如果是更新的远端节点，则需要更新Transporter中对应的Peer实例
			s.r.transport.UpdatePeer(m.ID, m.PeerURLs)
		}
	}
	return false, nil
}
// 该方法是真正生成快照文件的地方，其中会启动一个单独的后台goroutine来完成新快照文件的生成，主要是序列化V2存储中的数据并持久化到文件中，触发相应的压缩操作。
// TODO: non-blocking snapshot
func (s *EtcdServer) snapshot(snapi uint64, confState raftpb.ConfState) {
	clone := s.store.Clone()															//复制V2存储
	// commit kv to write metadata (for example: consistent index) to disk.
	// KV().commit() updates the consistent index in backend.
	// All operations that update consistent index must be called sequentially
	// from applyAll function.
	// So KV().Commit() cannot run in parallel with apply. It has to be called outside
	// the go routine created below.
	s.KV().Commit()																		//提交V3存储中当前等待读写事务

	s.goAttach(func() {
		d, err := clone.SaveNoCopy()													//将V2存储序列化成JSON数据
		// TODO: current store will never fail to do a snapshot
		// what should we do if the store might fail?
		if err != nil {
			plog.Panicf("store save should never fail: %v", err)
		}
		//将上述快照数据和元数据更新到etcd-raft模块中的MemoryStorage中，并且返回Snapshot实例(即MemoryStorage.snapshot字段)
		snap, err := s.r.raftStorage.CreateSnapshot(snapi, &confState, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == raft.ErrSnapOutOfDate {
				return
			}
			plog.Panicf("unexpected create snapshot error %v", err)
		}
		// SaveSnap saves the snapshot and releases the locked wal files	将V2存储的快照数据记录到磁盘中，该过程涉及在WAL日志文件中记录快照元数据及写入snap文件
		// to the snapshot index.                                           等操作。
		if err = s.r.storage.SaveSnap(snap); err != nil {
			plog.Fatalf("save snapshot error: %v", err)
		}
		plog.Infof("saved snapshot at index %d", snap.Metadata.Index)

		// When sending a snapshot, etcd will pause compaction.
		// After receives a snapshot, the slow follower needs to get all the entries right after		如果当前还存在已发送但未响应的快照消息，则不能进行后续的
		// the snapshot sent to catch up. If we do not pause compaction, the log entries right after	压缩操作，如果进行了后续的压缩，则可能导致Follower节点再次
		// the snapshot sent might already be compacted. It happens when the snapshot takes long time	无法追赶上Leader节点，从而需要再次发送快照数据
		// to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
		if atomic.LoadInt64(&s.inflightSnapshots) != 0 {
			plog.Infof("skip compaction since there is an inflight snapshot")
			return
		}

		// keep some in memory log entries for slow followers.			为了防止集群中存在比较慢的Follower节点，保留5000条Entry记录不压缩
		compacti := uint64(1)
		if snapi > numberOfCatchUpEntries {
			compacti = snapi - numberOfCatchUpEntries
		}
		err = s.r.raftStorage.Compact(compacti)							//压缩MemoryStorage中指定位置之前的全部Entry记录
		if err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			plog.Panicf("unexpected compaction error %v", err)
		}
		plog.Infof("compacted raft log at %d", compacti)
	})
}

// CutPeer drops messages to the specified peer.
func (s *EtcdServer) CutPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.CutPeer(id)
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (s *EtcdServer) MendPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.MendPeer(id)
	}
}

func (s *EtcdServer) PauseSending() { s.r.pauseSending() }

func (s *EtcdServer) ResumeSending() { s.r.resumeSending() }

func (s *EtcdServer) ClusterVersion() *semver.Version {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.Version()
}

// monitorVersions checks the member's version every monitorVersionInterval.
// It updates the cluster version if all members agrees on a higher one.
// It prints out log if there is a member with a higher version than the
// local version.
func (s *EtcdServer) monitorVersions() {
	for {
		select {
		case <-s.forceVersionC:
		case <-time.After(monitorVersionInterval):
		case <-s.stopping:
			return
		}

		if s.Leader() != s.ID() {
			continue
		}

		v := decideClusterVersion(getVersions(s.cluster, s.id, s.peerRt))
		if v != nil {
			// only keep major.minor version for comparison
			v = &semver.Version{
				Major: v.Major,
				Minor: v.Minor,
			}
		}

		// if the current version is nil:
		// 1. use the decided version if possible
		// 2. or use the min cluster version
		if s.cluster.Version() == nil {
			verStr := version.MinClusterVersion
			if v != nil {
				verStr = v.String()
			}
			s.goAttach(func() { s.updateClusterVersion(verStr) })
			continue
		}

		// update cluster version only if the decided version is greater than
		// the current cluster version
		if v != nil && s.cluster.Version().LessThan(*v) {
			s.goAttach(func() { s.updateClusterVersion(v.String()) })
		}
	}
}

func (s *EtcdServer) updateClusterVersion(ver string) {
	if s.cluster.Version() == nil {
		plog.Infof("setting up the initial cluster version to %s", version.Cluster(ver))
	} else {
		plog.Infof("updating the cluster version from %s to %s", version.Cluster(s.cluster.Version().String()), version.Cluster(ver))
	}
	req := pb.Request{
		Method: "PUT",
		Path:   membership.StoreClusterVersionKey(),
		Val:    ver,
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.Cfg.ReqTimeout())
	_, err := s.Do(ctx, req)
	cancel()
	switch err {
	case nil:
		return
	case ErrStopped:
		plog.Infof("aborting update cluster version because server is stopped")
		return
	default:
		plog.Errorf("error updating cluster version (%v)", err)
	}
}

func (s *EtcdServer) parseProposeCtxErr(err error, start time.Time) error {
	switch err {
	case context.Canceled:
		return ErrCanceled
	case context.DeadlineExceeded:
		s.leadTimeMu.RLock()
		curLeadElected := s.leadElectedTime
		s.leadTimeMu.RUnlock()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return ErrTimeoutDueToLeaderFail
		}

		lead := types.ID(atomic.LoadUint64(&s.r.lead))
		switch lead {
		case types.ID(raft.None):
			// TODO: return error to specify it happens because the cluster does not have leader now
		case s.ID():
			if !isConnectedToQuorumSince(s.r.transport, start, s.ID(), s.cluster.Members()) {
				return ErrTimeoutDueToConnectionLost
			}
		default:
			if !isConnectedSince(s.r.transport, start, lead) {
				return ErrTimeoutDueToConnectionLost
			}
		}

		return ErrTimeout
	default:
		return err
	}
}

func (s *EtcdServer) KV() mvcc.ConsistentWatchableKV { return s.kv }
func (s *EtcdServer) Backend() backend.Backend {
	s.bemu.Lock()
	defer s.bemu.Unlock()
	return s.be
}

func (s *EtcdServer) AuthStore() auth.AuthStore { return s.authStore }

func (s *EtcdServer) restoreAlarms() error {
	s.applyV3 = s.newApplierV3()
	as, err := alarm.NewAlarmStore(s)
	if err != nil {
		return err
	}
	s.alarmStore = as
	if len(as.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.applyV3 = newApplierV3Capped(s.applyV3)
	}
	if len(as.Get(pb.AlarmType_CORRUPT)) > 0 {
		s.applyV3 = newApplierV3Corrupt(s.applyV3)
	}
	return nil
}

func (s *EtcdServer) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *EtcdServer) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
}

func (s *EtcdServer) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&s.committedIndex)
}

func (s *EtcdServer) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&s.committedIndex, v)
}

// goAttach creates a goroutine on a given function and tracks it using		该方法会启动一个后台goroutine执行传入的函数。
// the etcdserver waitgroup.
func (s *EtcdServer) goAttach(f func()) {
	s.wgMu.RLock() // this blocks with ongoing close(s.stopping)
	defer s.wgMu.RUnlock()
	select {
	case <-s.stopping:				//检测当前EtcdServer实例是否已停止
		plog.Warning("server has stopped (skipping goAttach)")
		return
	default:
	}

	// now safe to add since waitgroup wait has not started yet
	s.wg.Add(1)				//调用EtcdServer.Stop()时，需要等待该后台goroutine结束之后才返回
	go func() {					//启动一个后台goroutine，执行传入的回调函数
		defer s.wg.Done()			//该后台goroutine结束时调用
		f()
	}()
}

func (s *EtcdServer) Alarms() []*pb.AlarmMember {
	return s.alarmStore.Get(pb.AlarmType_NONE)
}
