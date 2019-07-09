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
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"

	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	send(m raftpb.Message)						//发送单个消息，该方法是非阻塞的，如果出现发送失败，则会将失败信息报告给底层的Raft接口

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	sendSnap(m snap.Message)					//发送snap.Message

	// update updates the urls of remote peer.
	update(urls types.URLs)						//更新对应节点暴露的URL地址

	// attachOutgoingConn attaches the outgoing connection to the peer for    将指定的连接与当前Peer绑定，Peer会将该连接作为Stream消息通道使用。当Peer不再使用该
	// stream usage. After the call, the ownership of the outgoing            连接时，会将该连接关闭
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()																		//关闭当前Peer实例，会关闭底层的网络连接
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	// id of the remote raft peer node
	id types.ID								//该Peer实例对应的节点的ID
	r  Raft									//Raft接口，在Raft接口实现的底层封装了etcd-raft模块

	status *peerStatus

	picker *urlPicker						//每个节点可能提供多个URL供其他节点访问，当其中一个访问失败时，我们应该可以尝试访问另一个。而urlPicker提供的主要功能是在这些URL之间进行切换

	msgAppV2Writer *streamWriter
	writer         *streamWriter			//负责向Stream消息通道写入消息
	pipeline       *pipeline				//Pipeline消息通道
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages		负责发送快照数据
	msgAppV2Reader *streamReader
	msgAppReader   *streamReader			//负责从Stream消息通道读取消息

	//从Stream消息通道中读到消息之后，会通过该通道将消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理
	recvc chan raftpb.Message
	//从Stream消息通道中读取到MsgProp类型的消息之后，会通过该通道将MsgProp消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理
	propc chan raftpb.Message

	mu     sync.Mutex
	paused bool								//是否暂停向对应节点发送消息

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(transport *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	plog.Infof("starting peer %s...", peerID)
	defer plog.Infof("started peer %s", peerID)

	status := newPeerStatus(peerID)
	picker := newURLPicker(urls)				//根据节点提供的URL创建urlPicker
	errorc := transport.ErrorC
	r := transport.Raft							//底层的Raft状态机
	pipeline := &pipeline{						//创建pipeline实例
		peerID:        peerID,
		tr:            transport,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	pipeline.start()							//启动pipeline

	p := &peer{									//创建Peer实例
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(peerID, status, fs, r),
		writer:         startStreamWriter(peerID, status, fs, r),				//创建并启动streamWriter
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(transport, picker, peerID, status),
		recvc:          make(chan raftpb.Message, recvBufSize),					//创建recvc通道，注意缓冲区大小
		propc:          make(chan raftpb.Message, maxPendingProposals),			//创建propc通道，注意缓冲区大小
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	//启动单独的goroutine，它主要负责将recvc通道中读取消息，该通道中的消息就是从对端节点发送过来的消息，然后将读取到的消息交给底层的Raft状态机进行处理
	go func() {
		for {
			select {
			case mm := <-p.recvc:												//从recvc通道中获取连接上读取到的Message
				if err := r.Process(ctx, mm); err != nil {						//将Message交给底层Raft状态机处理，异常处理
					plog.Warningf("failed to process raft message (%v)", err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.        在底层Raft状态机处理MsgProp类型的Message时，可能会阻塞，所以启动
	// Thus propc must be put into a separate routine with recvc to avoid blocking   单独的goroutine来处理
	// processing other raft messages.
	go func() {
		for {
			select {
			case mm := <-p.propc:													//从propc通道中获取MsgProp类型的Message，将Message交给底层Raft状态机处理
				if err := r.Process(ctx, mm); err != nil {
					plog.Warningf("failed to process raft message (%v)", err)
				}
			case <-p.stopc:
				return
			}
		}
	}()
	//创建并启动streamReader实例，它主要负责从Stream消息通道上读取消息
	p.msgAppV2Reader = &streamReader{
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     transport,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(transport.DialRetryFrequency, 1),
	}
	p.msgAppReader = &streamReader{
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     transport,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(transport.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}

func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {						//检测paused字段，是否暂停对指定节点发送消息
		return
	}

	writec, name := p.pick(m)		//根据消息的类型选择合适的消息通道
	select {
	case writec <- m:				//将Message写入writec通道中，等待发送
	default:						//如果发送出现阻塞，则将信息报告给底层Raft状态机，这里也会根据消息类型选择合适的报告方法
		p.r.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			plog.MergeWarningf("dropped internal raft message to %s since %s's sending buffer is full (bad/overloaded network)", p.id, name)
		}
		plog.Debugf("dropped %s to %s since %s's sending buffer is full", m.Type, p.id, name)
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}
//该方法会单独启动一个线程并调用snapshotSender.send()方法完成快照数据的发送。
func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}
//该方法将底层网络连接传递到streamWriter中
func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:						//这里只关注最新版本的实现
		ok = p.writer.attach(conn)				//将conn实例交给streamWriter处理
	default:
		plog.Panicf("unhandled stream type %s", conn.t)
	}
	if !ok {									//出现异常则关闭连接
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	plog.Infof("stopping peer %s...", p.id)
	defer plog.Infof("stopped peer %s", p.id)

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block          如果是MsgSnap类型的消息，则返回Pipeline消息通道对应的Channel，否则返回Stream消息
	// stream for a long time, only use one of the N pipelines to send MsgSnap.   通道对应的Channel，如果Stream消息通道不可用，则使用Pipeline消息通道发送所有类型的消息
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
