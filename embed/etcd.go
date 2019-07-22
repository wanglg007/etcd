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

package embed

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	defaultLog "log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/compactor"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/etcdhttp"
	"github.com/coreos/etcd/etcdserver/api/v2http"
	"github.com/coreos/etcd/etcdserver/api/v2v3"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/etcdserver/api/v3rpc"
	"github.com/coreos/etcd/pkg/cors"
	"github.com/coreos/etcd/pkg/debugutil"
	runtimeutil "github.com/coreos/etcd/pkg/runtime"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/rafthttp"

	"github.com/coreos/pkg/capnslog"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "embed")

const (
	// internal fd usage includes disk usage and transport usage.
	// To read/write snapshot, snap pkg needs 1. In normal case, wal pkg needs
	// at most 2 to read/lock/write WALs. One case that it needs to 2 is to
	// read all logs after some snapshot index, which locates at the end of
	// the second last and the head of the last. For purging, it needs to read
	// directory, so it needs 1. For fd monitor, it needs 1.
	// For transport, rafthttp builds two long-polling connections and at most
	// four temporary connections with each member. There are at most 9 members
	// in a cluster, so it should reserve 96.
	// For the safety, we set the total reserved number to 150.
	reservedInternalFDNum = 150
)

// Etcd contains a running etcd server and its listeners.
type Etcd struct {
	Peers   []*peerListener
	Clients []net.Listener
	// a map of contexts for the servers that serves client requests.
	sctxs            map[string]*serveCtx
	metricsListeners []net.Listener

	Server *etcdserver.EtcdServer

	cfg   Config
	stopc chan struct{}
	errc  chan error

	closeOnce sync.Once
}

type peerListener struct {
	net.Listener
	serve func() error
	close func(context.Context) error
}
// 该函数负责启动etcd的服务端，每个节点都会对外提供两组URL地址，一组是与集群中其他节点交互的URL地址(Peer URL)，另一组是与客户端交互的URL地址(Client URL)。
// 该函数首先会为每个URL地址创建相应的Listener实例并记录到指定的字段中，然后调用etcdServer.NewServer()函数创建EtcdServer实例，之后调用EtcdServer.Start()方法
// 启动该实例。最后调用Etcd.serve()方法对外提供服务。
// StartEtcd launches the etcd server and HTTP handlers for client/server communication.
// The returned Etcd.Server is not guaranteed to have joined the cluster. Wait
// on the Etcd.Server.ReadyNotify() channel to know when it completes and is ready for use.
func StartEtcd(inCfg *Config) (e *Etcd, err error) {
	if err = inCfg.Validate(); err != nil {				//检测配置
		return nil, err
	}
	serving := false									//标识是否正在提供服务
	e = &Etcd{cfg: *inCfg, stopc: make(chan struct{})}//创建Etcd
	cfg := &e.cfg
	defer func() {
		if e == nil || err == nil {
			return
		}
		if !serving {
			// errored before starting gRPC server for serveCtx.serversC
			for _, sctx := range e.sctxs {
				close(sctx.serversC)
			}
		}
		e.Close()
		e = nil
	}()
	//初始化Etcd.Peers字段，其中为每个Peer URL创建相应的Listener实例
	if e.Peers, err = startPeerListeners(cfg); err != nil {
		return e, err
	}
	if e.sctxs, err = startClientListeners(cfg); err != nil {		//初始化Etcd.sctxs字段，其中为每个Client URL创建相应的Listener实例
		return e, err
	}
	for _, sctx := range e.sctxs {									//将sctxs字段中记录的Listener实例添加到Clients字段中
		e.Clients = append(e.Clients, sctx.l)
	}

	var (
		urlsmap types.URLsMap
		token   string
	)

	memberInitialized := true
	if !isMemberInitialized(cfg) {									//初始化集群中其他节点地址和自身的token
		memberInitialized = false
		urlsmap, token, err = cfg.PeerURLsMapAndToken("etcd")
		if err != nil {
			return e, fmt.Errorf("error setting up initial cluster: %v", err)
		}
	}

	// AutoCompactionRetention defaults to "0" if not set.
	if len(cfg.AutoCompactionRetention) == 0 {
		cfg.AutoCompactionRetention = "0"
	}
	autoCompactionRetention, err := parseCompactionRetention(cfg.AutoCompactionMode, cfg.AutoCompactionRetention)
	if err != nil {
		return e, err
	}

	srvcfg := etcdserver.ServerConfig{					//创建ServerConfig
		Name:                       cfg.Name,
		ClientURLs:                 cfg.ACUrls,
		PeerURLs:                   cfg.APUrls,
		DataDir:                    cfg.Dir,
		DedicatedWALDir:            cfg.WalDir,
		SnapCount:                  cfg.SnapCount,
		MaxSnapFiles:               cfg.MaxSnapFiles,
		MaxWALFiles:                cfg.MaxWalFiles,
		InitialPeerURLsMap:         urlsmap,
		InitialClusterToken:        token,
		DiscoveryURL:               cfg.Durl,
		DiscoveryProxy:             cfg.Dproxy,
		NewCluster:                 cfg.IsNewCluster(),
		ForceNewCluster:            cfg.ForceNewCluster,
		PeerTLSInfo:                cfg.PeerTLSInfo,
		TickMs:                     cfg.TickMs,
		ElectionTicks:              cfg.ElectionTicks(),
		InitialElectionTickAdvance: cfg.InitialElectionTickAdvance,
		AutoCompactionRetention:    autoCompactionRetention,
		AutoCompactionMode:         cfg.AutoCompactionMode,
		QuotaBackendBytes:          cfg.QuotaBackendBytes,
		MaxTxnOps:                  cfg.MaxTxnOps,
		MaxRequestBytes:            cfg.MaxRequestBytes,
		StrictReconfigCheck:        cfg.StrictReconfigCheck,
		ClientCertAuthEnabled:      cfg.ClientTLSInfo.ClientCertAuth,
		AuthToken:                  cfg.AuthToken,
		InitialCorruptCheck:        cfg.ExperimentalInitialCorruptCheck,
		CorruptCheckTime:           cfg.ExperimentalCorruptCheckTime,
		Debug:                      cfg.Debug,
	}

	if e.Server, err = etcdserver.NewServer(srvcfg); err != nil {		//创建EtcdServer实例
		return e, err
	}

	// buffer channel so goroutines on closed connections won't wait forever
	e.errc = make(chan error, len(e.Peers)+len(e.Clients)+2*len(e.sctxs))

	// newly started member ("memberInitialized==false")
	// does not need corruption check
	if memberInitialized {
		if err = e.Server.CheckInitialHashKV(); err != nil {
			// set "EtcdServer" to nil, so that it does not block on "EtcdServer.Close()"
			// (nothing to close since rafthttp transports have not been started)
			e.Server = nil
			return e, err
		}
	}
	e.Server.Start()

	if err = e.servePeers(); err != nil {
		return e, err
	}
	if err = e.serveClients(); err != nil {
		return e, err
	}
	if err = e.serveMetrics(); err != nil {
		return e, err
	}

	serving = true
	return e, nil
}

// Config returns the current configuration.
func (e *Etcd) Config() Config {
	return e.cfg
}

// Close gracefully shuts down all servers/listeners.
// Client requests will be terminated with request timeout.
// After timeout, enforce remaning requests be closed immediately.
func (e *Etcd) Close() {
	e.closeOnce.Do(func() { close(e.stopc) })

	// close client requests with request timeout
	timeout := 2 * time.Second
	if e.Server != nil {
		timeout = e.Server.Cfg.ReqTimeout()
	}
	for _, sctx := range e.sctxs {
		for ss := range sctx.serversC {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			stopServers(ctx, ss)
			cancel()
		}
	}

	for _, sctx := range e.sctxs {
		sctx.cancel()
	}

	for i := range e.Clients {
		if e.Clients[i] != nil {
			e.Clients[i].Close()
		}
	}

	for i := range e.metricsListeners {
		e.metricsListeners[i].Close()
	}

	// close rafthttp transports
	if e.Server != nil {
		e.Server.Stop()
	}

	// close all idle connections in peer handler (wait up to 1-second)
	for i := range e.Peers {
		if e.Peers[i] != nil && e.Peers[i].close != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			e.Peers[i].close(ctx)
			cancel()
		}
	}
}

func stopServers(ctx context.Context, ss *servers) {
	shutdownNow := func() {
		// first, close the http.Server
		ss.http.Shutdown(ctx)
		// then close grpc.Server; cancels all active RPCs
		ss.grpc.Stop()
	}

	// do not grpc.Server.GracefulStop with TLS enabled etcd server
	// See https://github.com/grpc/grpc-go/issues/1384#issuecomment-317124531
	// and https://github.com/coreos/etcd/issues/8916
	if ss.secure {
		shutdownNow()
		return
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		// close listeners to stop accepting new connections,
		// will block on any existing transports
		ss.grpc.GracefulStop()
	}()

	// wait until all pending RPCs are finished
	select {
	case <-ch:
	case <-ctx.Done():
		// took too long, manually close open transports
		// e.g. watch streams
		shutdownNow()

		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func (e *Etcd) Err() <-chan error { return e.errc }
//该函数会为每个Peer URL创建相应的Listener实例。后面会使用这些Listener创建相应的http.Server实例，从而监听Peer URL上的请求。
func startPeerListeners(cfg *Config) (peers []*peerListener, err error) {
	if err = updateCipherSuites(&cfg.PeerTLSInfo, cfg.CipherSuites); err != nil {
		return nil, err
	}
	if err = cfg.PeerSelfCert(); err != nil {
		plog.Fatalf("could not get certs (%v)", err)
	}
	if !cfg.PeerTLSInfo.Empty() {
		plog.Infof("peerTLS: %s", cfg.PeerTLSInfo)
	}

	peers = make([]*peerListener, len(cfg.LPUrls))				//在Config.LPUrls中记录了当前节点与集群中其他节点交互的URL地址
	defer func() {
		if err == nil {
			return
		}
		for i := range peers {
			if peers[i] != nil && peers[i].close != nil {
				plog.Info("stopping listening for peers on ", cfg.LPUrls[i].String())
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				peers[i].close(ctx)
				cancel()
			}
		}
	}()

	for i, u := range cfg.LPUrls {				//为LPUrls中的每个URL地址都创建相应的Listener
		if u.Scheme == "http" {
			if !cfg.PeerTLSInfo.Empty() {
				plog.Warningf("The scheme of peer url %s is HTTP while peer key/cert files are presented. Ignored peer key/cert files.", u.String())
			}
			if cfg.PeerTLSInfo.ClientCertAuth {
				plog.Warningf("The scheme of peer url %s is HTTP while client cert auth (--peer-client-cert-auth) is enabled. Ignored client cert auth for this url.", u.String())
			}
		}
		peers[i] = &peerListener{close: func(context.Context) error { return nil }}
		peers[i].Listener, err = rafthttp.NewListener(u, &cfg.PeerTLSInfo)
		if err != nil {
			return nil, err
		}
		// once serve, overwrite with 'http.Server.Shutdown'
		peers[i].close = func(context.Context) error {
			return peers[i].Listener.Close()
		}
		plog.Info("listening for peers on ", u.String())
	}
	return peers, nil
}

// configure peer handlers after rafthttp.Transport started
func (e *Etcd) servePeers() (err error) {
	ph := etcdhttp.NewPeerHandler(e.Server)							//注册Handler实例(创建PeerHandler)
	var peerTLScfg *tls.Config
	if !e.cfg.PeerTLSInfo.Empty() {
		if peerTLScfg, err = e.cfg.PeerTLSInfo.ServerConfig(); err != nil {
			return err
		}
	}

	for _, p := range e.Peers {									//使用Etcd.Peers中记录的Listener，创建相应的http.Server实例
		gs := v3rpc.Server(e.Server, peerTLScfg)
		m := cmux.New(p.Listener)
		go gs.Serve(m.Match(cmux.HTTP2()))
		srv := &http.Server{										//为每个URL地址创建一个http.Server
			Handler:     grpcHandlerFunc(gs, ph),
			ReadTimeout: 5 * time.Minute,
			ErrorLog:    defaultLog.New(ioutil.Discard, "", 0), // do not log user error
		}
		go srv.Serve(m.Match(cmux.Any()))
		p.serve = func() error { return m.Serve() }				//设置启动Http服务端的回调函数，并未真正启动
		p.close = func(ctx context.Context) error {					//设置关闭HTTP服务端的回调函数
			// gracefully shutdown http.Server
			// close open listeners, idle connections
			// until context cancel or time-out
			stopServers(ctx, &servers{secure: peerTLScfg != nil, grpc: gs, http: srv})
			return nil
		}
	}

	// start peer servers in a goroutine			 每个Peer URL对应的http.Server实例都会在单独的goroutine中启动
	for _, pl := range e.Peers {
		go func(l *peerListener) {
			e.errHandler(l.serve())					//调用前面的serve()回调函数，启动http.Server实例
		}(pl)
	}
	return nil
}
//该函数会为每个Client URL地址创建相应的serverCtx实例，在serverCtx实例中记录了对应的Client URL、相应的Listener实例和用户自定义Handler等信息。
func startClientListeners(cfg *Config) (sctxs map[string]*serveCtx, err error) {
	if err = updateCipherSuites(&cfg.ClientTLSInfo, cfg.CipherSuites); err != nil {
		return nil, err
	}
	if err = cfg.ClientSelfCert(); err != nil {
		plog.Fatalf("could not get certs (%v)", err)
	}
	if cfg.EnablePprof {
		plog.Infof("pprof is enabled under %s", debugutil.HTTPPrefixPProf)
	}

	sctxs = make(map[string]*serveCtx)				//该map用来记录Client URL与对应serveCtx实例的对应关系
	for _, u := range cfg.LCUrls {
		sctx := newServeCtx()						//每个Client URL都对应一个serveCtx实例

		if u.Scheme == "http" || u.Scheme == "unix" {
			if !cfg.ClientTLSInfo.Empty() {
				plog.Warningf("The scheme of client url %s is HTTP while peer key/cert files are presented. Ignored key/cert files.", u.String())
			}
			if cfg.ClientTLSInfo.ClientCertAuth {
				plog.Warningf("The scheme of client url %s is HTTP while client cert auth (--client-cert-auth) is enabled. Ignored client cert auth for this url.", u.String())
			}
		}
		if (u.Scheme == "https" || u.Scheme == "unixs") && cfg.ClientTLSInfo.Empty() {
			return nil, fmt.Errorf("TLS key/cert (--cert-file, --key-file) must be provided for client url %s with HTTPs scheme", u.String())
		}

		proto := "tcp"
		addr := u.Host
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			proto = "unix"
			addr = u.Host + u.Path
		}

		sctx.secure = u.Scheme == "https" || u.Scheme == "unixs"
		sctx.insecure = !sctx.secure
		if oldctx := sctxs[addr]; oldctx != nil {
			oldctx.secure = oldctx.secure || sctx.secure
			oldctx.insecure = oldctx.insecure || sctx.insecure
			continue
		}

		if sctx.l, err = net.Listen(proto, addr); err != nil {			//创建Listener
			return nil, err
		}
		// net.Listener will rewrite ipv4 0.0.0.0 to ipv6 [::], breaking
		// hosts that disable ipv6. So, use the address given by the user.
		sctx.addr = addr												//更新addr字段

		if fdLimit, fderr := runtimeutil.FDLimit(); fderr == nil {		//根据不同的系统和协议，对sctx.1中记录的Listener进行调整
			if fdLimit <= reservedInternalFDNum {
				plog.Fatalf("file descriptor limit[%d] of etcd process is too low, and should be set higher than %d to ensure internal usage", fdLimit, reservedInternalFDNum)
			}
			sctx.l = transport.LimitListener(sctx.l, int(fdLimit-reservedInternalFDNum))
		}

		if proto == "tcp" {
			if sctx.l, err = transport.NewKeepAliveListener(sctx.l, "tcp", nil); err != nil {
				return nil, err
			}
		}

		plog.Info("listening for client requests on ", u.Host)
		defer func() {
			if err != nil {
				sctx.l.Close()
				plog.Info("stopping listening for client requests on ", u.Host)
			}
		}()
		for k := range cfg.UserHandlers {						//设置用户自定义的Handler
			sctx.userHandlers[k] = cfg.UserHandlers[k]
		}
		sctx.serviceRegister = cfg.ServiceRegister
		if cfg.EnablePprof || cfg.Debug {
			sctx.registerPprof()
		}
		if cfg.Debug {
			sctx.registerTrace()
		}
		sctxs[addr] = sctx										//记录当前serverCtx实例
	}
	return sctxs, nil
}

func (e *Etcd) serveClients() (err error) {
	if !e.cfg.ClientTLSInfo.Empty() {
		plog.Infof("ClientTLS: %s", e.cfg.ClientTLSInfo)
	}

	if e.cfg.CorsInfo.String() != "" {
		plog.Infof("cors = %s", e.cfg.CorsInfo)
	}

	// Start a client server goroutine for each listen address
	var h http.Handler															//初始化V2版本API使用的Handler实例
	if e.Config().EnableV2 {
		if len(e.Config().ExperimentalEnableV2V3) > 0 {
			srv := v2v3.NewServer(v3client.New(e.Server), e.cfg.ExperimentalEnableV2V3)
			h = v2http.NewClientHandler(srv, e.Server.Cfg.ReqTimeout())
		} else {
			h = v2http.NewClientHandler(e.Server, e.Server.Cfg.ReqTimeout())	//注册完整的V2版本的Handler，可以正常响应的Client V2的请求
		}
	} else {
		mux := http.NewServeMux()
		etcdhttp.HandleBasic(mux, e.Server)										//只提供基本的查询功能，不响应Client V2的请求
		h = mux
	}
	h = http.Handler(&cors.CORSHandler{Handler: h, Info: e.cfg.CorsInfo})

	gopts := []grpc.ServerOption{}
	if e.cfg.GRPCKeepAliveMinTime > time.Duration(0) {
		gopts = append(gopts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             e.cfg.GRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}))
	}
	if e.cfg.GRPCKeepAliveInterval > time.Duration(0) &&
		e.cfg.GRPCKeepAliveTimeout > time.Duration(0) {
		gopts = append(gopts, grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    e.cfg.GRPCKeepAliveInterval,
			Timeout: e.cfg.GRPCKeepAliveTimeout,
		}))
	}

	// start client servers in a goroutine			每个Client URL对应的服务都会在单独的goroutine中启动
	for _, sctx := range e.sctxs {
		go func(s *serveCtx) {
			e.errHandler(s.serve(e.Server, &e.cfg.ClientTLSInfo, h, e.errHandler, gopts...))	//调用serveCtx.serve()方法完成启动
		}(sctx)
	}
	return nil
}

func (e *Etcd) serveMetrics() (err error) {
	if e.cfg.Metrics == "extensive" {
		grpc_prometheus.EnableHandlingTimeHistogram()
	}

	if len(e.cfg.ListenMetricsUrls) > 0 {
		metricsMux := http.NewServeMux()
		etcdhttp.HandleMetricsHealth(metricsMux, e.Server)

		for _, murl := range e.cfg.ListenMetricsUrls {
			tlsInfo := &e.cfg.ClientTLSInfo
			if murl.Scheme == "http" {
				tlsInfo = nil
			}
			ml, err := transport.NewListener(murl.Host, murl.Scheme, tlsInfo)
			if err != nil {
				return err
			}
			e.metricsListeners = append(e.metricsListeners, ml)
			go func(u url.URL, ln net.Listener) {
				plog.Info("listening for metrics on ", u.String())
				e.errHandler(http.Serve(ln, metricsMux))
			}(murl, ml)
		}
	}
	return nil
}

func (e *Etcd) errHandler(err error) {
	select {
	case <-e.stopc:
		return
	default:
	}
	select {
	case <-e.stopc:
	case e.errc <- err:
	}
}

func parseCompactionRetention(mode, retention string) (ret time.Duration, err error) {
	h, err := strconv.Atoi(retention)
	if err == nil {
		switch mode {
		case compactor.ModeRevision:
			ret = time.Duration(int64(h))
		case compactor.ModePeriodic:
			ret = time.Duration(int64(h)) * time.Hour
		}
	} else {
		// periodic compaction
		ret, err = time.ParseDuration(retention)
		if err != nil {
			return 0, fmt.Errorf("error parsing CompactionRetention: %v", err)
		}
	}
	return ret, nil
}
