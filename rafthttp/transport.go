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

package rafthttp

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/logutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"

	"github.com/coreos/pkg/capnslog"
	"github.com/xiang90/probing"
	"golang.org/x/time/rate"
)

var plog = logutil.NewMergeLogger(capnslog.NewPackageLogger("github.com/coreos/etcd", "rafthttp"))

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error				//将指定的消息实例传递到底层的etcd-raft模块进行处理
	IsIDRemoved(id uint64) bool											//检测指定节点是否从当前集群中移出
	ReportUnreachable(id uint64)										//通知底层etcd-raft模块，当前节点与指定的节点无法连通
	ReportSnapshot(id uint64, status raft.SnapshotStatus)				//通知底层etcd-raft模块，快照数据是否发送成功
}

type Transporter interface {											//定义了etcd网络层的核心功能
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	Start() error														//初始化操作
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	Handler() http.Handler												//创建Handler实例，并关联到指定的URL上
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send(m []raftpb.Message)											//发送消息
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	SendSnapshot(m snap.Message)										//发送快照数据
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	AddRemote(id types.ID, urls []string)								//在集群中添加一个节点时，其他节点会通过该方法添加该新加入节点的信息
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(id types.ID, urls []string)
	// RemovePeer removes the peer with given id.
	RemovePeer(id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	UpdatePeer(id types.ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id types.ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	Stop()																//关闭操作，该方法会关闭全部的网络连接
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
type Transport struct {
	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	ID          types.ID   // local member ID							当前节点自己的ID
	URLs        types.URLs // local peer URLs							当前节点与集群中其他节点交互时使用的URL地址
	ClusterID   types.ID   // raft cluster ID for request validation	当前节点所在的集群的ID
	//Raft是接口，其实现的底层封装了etcd-raft模块，让rafthttp.Transport收到消息之后，会将其交给Raft实例进行处理
	Raft        Raft       // raft state machine, to which the Transport forwards received messages and reports status
	Snapshotter *snap.Snapshotter										//负责管理快照文件
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// used to record transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams		Stream消息通道中使用的http.RoundTripper实例
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu      sync.RWMutex         // protect the remote and peer map
	//remote中只封装了pipeline实例，remote主要负责发送快照数据，帮助新加入的节点快速追赶上其他节点的数据
	remotes map[types.ID]*remote // remotes map that helps newly joined member to catch up
	//Peer接口是当前节点对集群中其他节点的抽象表示。对于当前节点来说，集群中其他节点在本地都会有一个Peer实例与之对应，peers字段维护了节点ID到对应Peer实例之间的映射关系
	peers   map[types.ID]Peer    // peers map

	pipelineProber probing.Prober										//用于探测pipeline消息通道是否可用
	streamProber   probing.Prober
}

func (t *Transport) Start() error {
	var err error
	//创建Stream消息通道使用的http.RoundTripper实例，底层实际上是创建http.Transport实例。参数分别为：创建连接的超时时间、读写请求的超时时间和keepAlive时间
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	//创建Pipeline消息通道使用的http.RoundTripper实例。与streamRt不同的是，读写请求的超时时间设置成了永不过期
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	//初始化remotes和peers两个map字段
	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	t.pipelineProber = probing.NewProber(t.pipelineRt)			//用于探测Pipeline消息通道是否可用
	t.streamProber = probing.NewProber(t.streamRt)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}
//该方法主要负责创建Stream消息通道和Pipeline消息通道用到的Handler实例，并注册到相应的请求路径。
func (t *Transport) Handler() http.Handler {
	//创建pipelineHandler、streamHandler和snapshotHandler三个实例，这三个实例都实现了http.Server.Handler接口
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.ID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID)
	mux := http.NewServeMux()							//创建ServeMux实例，它是一个多路复用器，其通过m字段存储具体的URL和Handler实例之间的映射关系
	//下面就是设置URL与Handler实例的映射关系，也就是访问指定URL地址的请求，由对应的Handler实例进行处理
	mux.Handle(RaftPrefix, pipelineHandler)
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	mux.Handle(RaftSnapshotPrefix, snapHandler)
	mux.Handle(ProbingPrefix, probing.NewHandler())
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}
//该方法负责发送指定的raftpb.Message消息，其中首先尝试使用目标节点对应的Peer实例发送消息，如果没有找到对应的Peer实例，则尝试使用对应的remote实例发送消息。
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {						//遍历msgs切片中的全部消息
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := types.ID(m.To)						//根据raftpb.Message.To字段，获取目标节点对应的Peer实例

		t.mu.RLock()								//加锁
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()								//解锁

		if pok {									//如果存在对应的Peer实例，则使用Peer发送消息
			if m.Type == raftpb.MsgApp {			//统计信息
				t.ServerStats.SendAppendReq(m.Size())
			}
			p.send(m)								//通过peer.send()方法完成消息的发送
			continue
		}

		if rok {									//如果指定节点ID不存在对应的Peer实例，则尝试使用查找对应remote实例
			g.send(m)								//通过remote.send()方法完成消息的发送
			continue
		}
		//执行到这里表示无法找到raftpb.Message的目标节点，则记录日志信息
		plog.Debugf("ignored message %s (sent to unknown peer %s)", m.Type, to)
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}
	t.pipelineProber.RemoveAll()
	t.streamProber.RemoveAll()
	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

func (t *Transport) AddRemote(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.remotes[id] = startRemote(t, urls, id)
}
//该函数的主要工作是创建并启动对应节点的Peer实例
func (t *Transport) AddPeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {						//检测当前rafthttp.Transport的运行状态
		panic("transport stopped")
	}
	if _, ok := t.peers[id]; ok {			//是否已经与指定ID的节点建立了连接
		return
	}
	urls, err := types.NewURLs(us)			//解析us切片中指定URL连接
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	fs := t.LeaderStats.Follower(id.String())
	//创建指定节点对应的Peer实例，其中会相关的Stream消息通道和Pipeline消息通道
	t.peers[id] = startPeer(t, urls, id, fs)
	//每隔一段时间，prober会向该节点发送探测消息，检测对端的健康状况
	addPeerToProber(t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rtts)
	addPeerToProber(t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rtts)
	plog.Infof("added peer %s", id)
}

func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		plog.Panicf("unexpected removal of unknown peer '%d'", id)
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())
	t.pipelineProber.Remove(id.String())
	t.streamProber.Remove(id.String())
	plog.Infof("removed peer %s", id)
}

func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.peers[id].update(urls)

	t.pipelineProber.Remove(id.String())
	addPeerToProber(t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rtts)
	t.streamProber.Remove(id.String())
	addPeerToProber(t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rtts)
	plog.Infof("updated peer %s", id)
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}
	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}

type nopTransporter struct{}

func NewNopTransporter() Transporter {
	return &nopTransporter{}
}

func (s *nopTransporter) Start() error                        { return nil }
func (s *nopTransporter) Handler() http.Handler               { return nil }
func (s *nopTransporter) Send(m []raftpb.Message)             {}
func (s *nopTransporter) SendSnapshot(m snap.Message)         {}
func (s *nopTransporter) AddRemote(id types.ID, us []string)  {}
func (s *nopTransporter) AddPeer(id types.ID, us []string)    {}
func (s *nopTransporter) RemovePeer(id types.ID)              {}
func (s *nopTransporter) RemoveAllPeers()                     {}
func (s *nopTransporter) UpdatePeer(id types.ID, us []string) {}
func (s *nopTransporter) ActiveSince(id types.ID) time.Time   { return time.Time{} }
func (s *nopTransporter) ActivePeers() int                    { return 0 }
func (s *nopTransporter) Stop()                               {}
func (s *nopTransporter) Pause()                              {}
func (s *nopTransporter) Resume()                             {}

type snapTransporter struct {
	nopTransporter
	snapDoneC chan snap.Message
	snapDir   string
}

func NewSnapTransporter(snapDir string) (Transporter, <-chan snap.Message) {
	ch := make(chan snap.Message, 1)
	tr := &snapTransporter{snapDoneC: ch, snapDir: snapDir}
	return tr, ch
}

func (s *snapTransporter) SendSnapshot(m snap.Message) {
	ss := snap.New(s.snapDir)
	ss.SaveDBFrom(m.ReadCloser, m.Snapshot.Metadata.Index+1)
	m.CloseWithError(nil)
	s.snapDoneC <- m
}
