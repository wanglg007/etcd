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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/httputil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/version"
	"github.com/coreos/go-semver/semver"
)

const (
	streamTypeMessage  streamType = "message"
	streamTypeMsgAppV2 streamType = "msgappv2"

	streamBufSize = 4096
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")

	// the key is in string format "major.minor.patch"
	supportedStream = map[string][]streamType{
		"2.0.0": {},
		"2.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.0.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.3.0": {streamTypeMsgAppV2, streamTypeMessage},
	}
)

type streamType string

func (t streamType) endpoint() string {
	switch t {
	case streamTypeMsgAppV2:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		plog.Panicf("unhandled stream type %v", t)
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMsgAppV2:
		return "stream MsgApp v2"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = raftpb.Message{Type: raftpb.MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer
}
// 主要功能是向Stream消息通道写入消息
// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	peerID types.ID							//对端节点的ID
	status *peerStatus
	fs     *stats.FollowerStats
	r      Raft								//底层的Raft实例

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer						//负责关闭底层的长连接
	working bool							//负责标识当前的streamWriteer是否可用(底层是否关联了相应的网络连接)

	msgc  chan raftpb.Message				//Peer会将待发送的消息写入该通道，streamWriter则从该通道中读取消息并发送出去
	connc chan *outgoingConn				//通过该通道获取当前streamWriter实例关联的底层网络连接。outgongConn其实是对网络连接的一层封装，其中记录当前连接使用的协议版本，以及用于关闭连接的Flusher和Closer等信息
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(id types.ID, status *peerStatus, fs *stats.FollowerStats, r Raft) *streamWriter {
	w := &streamWriter{
		peerID: id,
		status: status,
		fs:     fs,
		r:      r,
		msgc:   make(chan raftpb.Message, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}
//该方法主要完成三件事情：(1)当其他节点主动与当前节点创建连接(即Stream消息通道底层使用的网络连接)时，该连接实例会写入对应peer.writer.connc通道，在streamWriter.run()
//方法中会通过该通道获取该连接实例并进行绑定，之后才能开始后续的消息发送；(2)定时发送心跳消息，该心跳消息不是etcd-raft模块提到的MsgHeartbeart消息，而是为了防止底层
//连接超时的消息；(3)发送除心跳消息外的其他类型的消息；
func (cw *streamWriter) run() {
	var (
		msgc       chan raftpb.Message				//指向当前streamWriter.msgc字段
		//定时器会定时向该通道发送信号，触发心跳消息的发送。该心跳消息的主要目的为了防止连接长时间不用断开的。
		heartbeatc <-chan time.Time
		t          streamType						//用来记录消息的版本消息
		enc        encoder							//编码器，负责将消息序列化并写入连接的缓冲区
		flusher    http.Flusher						//负责刷新底层连接，将数据真正发送出去
		batched    int								//当前未Flush的消息个数
	)
	tickc := time.NewTicker(ConnReadTimeout / 3)	//发送心跳消息的定时器
	defer tickc.Stop()
	unflushed := 0									//未Flush的字节数

	plog.Infof("started streaming with peer %s (writer)", cw.peerID)

	for {
		//当其他节点主动与当前节点创建Stream消息通道时，会先通过StreamHandler的处理。StreamHandler会通过attach()方法将连接写入对应peer.writer.connc通道，而当前
		//的goroutine会通过该通道获取连接，然后开始发送消息
		select {
		case <-heartbeatc:								//定时器到期，触发心跳消息
			err := enc.encode(&linkHeartbeatMessage)	//通过encoder将心跳消息
			unflushed += linkHeartbeatMessage.Size()	//增加未Flush出去的字节数
			//若没有异常，则使用flusher将缓存的消息全部发送出去，并重置batched和unflushed两个统计变量
			if err == nil {
				flusher.Flush()
				batched = 0
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
				unflushed = 0
				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close()						//若发送异常，则关闭streamWriter，会导致底层连接的关闭
			plog.Warningf("lost the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			heartbeatc, msgc = nil, nil		//将heartbeatc和msgc两个通道清空，后续就不会再发送心跳消息和其他类型的消息

		case m := <-msgc:					//peer向streamWriter.msgc写入待发送的消息
			err := enc.encode(&m)			//将消息序列化并写入底层连接
			if err == nil {					//若没有异常，则递增unflushed变量
				unflushed += m.Size()
				//msgc通道中的消息全部发送完成或是未Flush的消息较多，则触发Flush，否则只是递增batched变量
				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}
			//若发生异常，则关闭streamWriter,清空heartbeatc和msgc两个通道
			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			plog.Warningf("lost the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		case conn := <-cw.connc:									//获取与当前streamWriter实例绑定的底层连接
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t												//获取该连接底层发送的消息版本，并创建相应的encoder实例
			switch conn.t {
			case streamTypeMsgAppV2:
				enc = newMsgAppV2Encoder(conn.Writer, cw.fs)
			case streamTypeMessage:									//这里只关注最新版本，忽略其他版本
				//将http.ResponseWriter封装成messageEncoder，上层调用通过messageEncoder实例完成消息发送
				enc = &messageEncoder{w: conn.Writer}
			default:
				plog.Panicf("unhandled stream type %s", conn.t)
			}
			flusher = conn.Flusher									//记录底层连接对应的Flusher
			unflushed = 0											//重置未Flush的字节数
			cw.status.activate()									//peerStatus.active设置为true
			cw.closer = conn.Closer									//记录底层连接对应的Flusher
			cw.working = true										//标识当前streamWriter正在运行
			cw.mu.Unlock()

			if closed {
				plog.Warningf("closed an existing TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			plog.Infof("established a TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			heartbeatc, msgc = tickc.C, cw.msgc						//更新heartbeatc和msgc两个通道，自此之后，才能发送消息
		case <-cw.stopc:
			if cw.close() {
				plog.Infof("closed the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			plog.Infof("stopped streaming with peer %s (writer)", cw.peerID)
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- raftpb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		plog.Errorf("peer %s (writer) connection close error: %v", cw.peerID, err)
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan raftpb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {					//主要功能是从Stream通道中读取消息
	peerID types.ID							//对应节点的ID
	typ    streamType						//关联的底层连接使用的协议版本信息

	tr     *Transport						//关联的rafthttp.Transport实例
	picker *urlPicker						//用于获取对端节点的可用的URL
	status *peerStatus
	//从对端发送来的非MsgProp类型的消息会首先由streamReader写入recvc通道中，然后由peer.start()启动的后台goroutine读取出来，交由底层的etcd-raft模块进行处理
	recvc  chan<- raftpb.Message
	//该通道接收的是MsgProp类型的消息
	propc  chan<- raftpb.Message

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool								//是否暂停读取消息
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ								//获取使用的消息版本
	plog.Infof("started streaming with peer %s (%s reader)", cr.peerID, t)
	for {
		//向对端节点发送一个Get请求，然后获取并返回相应的ReadCloser
		rc, err := cr.dial(t)
		if err != nil {
			if err != errUnsupportedStreamType {
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {						//若未出现异常，则开始读取对端返回的消息，并将读取到的消息写入streamReader.recvc通道中
			cr.status.activate()
			plog.Infof("established a TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			err = cr.decodeLoop(rc, t)	//该方法退出可能是对端主动关闭连接，此时需要等待100ms后再创建新连接，这主要是为了防止频繁重试
			plog.Warningf("lost the TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			plog.Infof("stopped streaming with peer %s (%s reader)", cr.peerID, t)
			close(cr.done)
			return
		}
		if err != nil {
			plog.Errorf("streaming with peer %s (%s reader) rate limiter error: %v", cr.peerID, t, err)
		}
	}
}
//该方法会从底层的网络连接读取数据并进行反序列化，之后得到的消息实例写入recvc通道(或propc通道)中，等待Peer进行处理。
func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	//根据使用的协议版本创建对应的decoder实例
	switch t {
	case streamTypeMsgAppV2:
		dec = newMsgAppV2Decoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}					//messageDecoder主要负责从连接中读取数据
	default:
		plog.Panicf("unhandled stream type %s", t)
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()
	//检测streamReader是否已经关闭，若关闭则返回异常
	for {
		m, err := dec.decode()						//从底层连接中读取数据，并反序列化成raftpb.Message实例
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(m.Size()))

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {									//检测是否已经暂停从该连接上读取消息
			continue
		}

		if isLinkHeartbeatMessage(&m) {				//忽略连接层的心跳消息
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		recvc := cr.recvc							//根据读取到的消息类型，选择对应通道进行写入
		if m.Type == raftpb.MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m:							//将消息写入对应的通道中，之后会交给底层的Raft状态机进行处理
		default:
			if cr.status.isActive() {
				plog.MergeWarningf("dropped internal raft message from %s since receiving buffer is full (overloaded network)", types.ID(m.From))
			}
			plog.Debugf("dropped %s from %s since receiving buffer is full", m.Type, types.ID(m.From))
			recvFailures.WithLabelValues(types.ID(m.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}
//该方法主要负责与对端节点建立连接
func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick()									//获取对端节点暴露的一个URL
	uu := u
	uu.Path = path.Join(t.endpoint(), cr.tr.ID.String())	//根据使用协议的版本和节点ID创建最终的URL地址

	req, err := http.NewRequest("GET", uu.String(), nil)		//创建一个Get请求
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	req.Header.Set("X-Server-From", cr.tr.ID.String())						//设置请求头
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())

	setPeerURLsHeader(req, cr.tr.URLs)											//将当前节点暴露的URL也一起发送给对端节点

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.streamRt.RoundTrip(req)									//发送请求，ping-pong
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	rv := serverVersion(resp.Header)
	lv := semver.Must(semver.NewVersion(version.Version))
	if compareMajorMinorVersion(rv, lv) == -1 && !checkStreamSupport(rv, t) {
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}
	//解析HTTP响应，检测版本信息
	switch resp.StatusCode {								//根据HTTP的响应码进行处理
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.ID)
	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			plog.Errorf("request sent was ignored by peer %s (server version incompatible)", cr.peerID)
			return nil, errIncompatibleVersion
		case errClusterIDMismatch.Error():
			plog.Errorf("request sent was ignored (cluster ID mismatch: peer[%s]=%s, local=%s)",
				cr.peerID, resp.Header.Get("X-Etcd-Cluster-ID"), cr.tr.ClusterID)
			return nil, errClusterIDMismatch
		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}
	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			plog.Errorf("peer %s (reader) connection close error: %v", cr.peerID, err)
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}

// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *semver.Version, t streamType) bool {
	nv := &semver.Version{Major: v.Major, Minor: v.Minor}
	for _, s := range supportedStream[nv.String()] {
		if s == t {
			return true
		}
	}
	return false
}
