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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"time"

	pioutil "github.com/coreos/etcd/pkg/ioutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/version"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix         = "/raft"
	ProbingPrefix      = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix   = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix = path.Join(RaftPrefix, "snapshot")

	errIncompatibleVersion = errors.New("incompatible version")
	errClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

type pipelineHandler struct {
	tr  Transporter				//当前pipeline实例关联的rafthttp.Transport实例
	r   Raft					//底层的Raft实例
	cid types.ID				//当前集群的ID
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(tr Transporter, r Raft, cid types.ID) http.Handler {
	return &pipelineHandler{
		tr:  tr,
		r:   r,
		cid: cid,
	}
}
//该方法通过读取对端节点发来的请求得到相应的消息实例，然后将其交给底层的etcd-raft模块进行处理。
func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//首先进行一系列的检查，例如：检查请求的Method是否为POST，检测集群ID是否合法等
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	// Limit the data size that could be read from the request body, which ensures that read from       限制每次从底层连接读取的字节数上限，默认是64KB，因为快照数据可能非常大，为了防止读取超时，
	// connection will not time out accidentally due to possible blocking in underlying implementation. 只能每次读取一部分数据到缓冲区中，最后将全部数据拼接起来，得到完整的快照数据
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)									//读取HTTP请求的Body的全部内容
	if err != nil {
		plog.Errorf("failed to read raft message (%v)", err)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {								//反序列化得到raftpb.Message实例
		plog.Errorf("failed to unmarshal raft message (%v)", err)
		http.Error(w, "error unmarshaling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	if err := h.r.Process(context.TODO(), m); err != nil {				//将读取到的消息实例交给底层的Raft状态机进行处理
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			plog.Warningf("failed to process raft message (%v)", err)
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
			panic(err)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)									//向对端节点返回合适的状态码，表示请求已经被处理
}
//用来接收对端节点发来的快照数据
type snapshotHandler struct {
	tr          Transporter					//关联的Transporter实例
	r           Raft						//底层的Raft实例
	snapshotter *snap.Snapshotter			//负责将快照数据保存到本地文件中
	cid         types.ID					//集群的ID
}

func newSnapshotHandler(tr Transporter, r Raft, snapshotter *snap.Snapshotter, cid types.ID) http.Handler {
	return &snapshotHandler{
		tr:          tr,
		r:           r,
		snapshotter: snapshotter,
		cid:         cid,
	}
}

const unknownSnapshotSender = "UNKNOWN_SNAPSHOT_SENDER"

// ServeHTTP serves HTTP request to receive and process snapshot message.
//
// If request sender dies without closing underlying TCP connection,
// the handler will keep waiting for the request body until TCP keepalive
// finds out that the connection is broken after several minutes.
// This is acceptable because
// 1. snapshot messages sent through other TCP connections could still be
// received and processed.
// 2. this case should happen rarely, so no further optimization is done.
// 该方法除了读取对端节点发来的快照数据，还会在本地生成相应的快照文件，并将快照数据通过Raft接口传递给底层的ertcd-raft模块进行处理。
func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	//首先进行一系列的检查，例如：检查请求的Method是否为POST
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	addRemoteFromRequest(h.tr, r)

	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	//限制每次从底层连接读取的字节数上限，默认为64KB，因为快照数据可能非常大，为了防止读取超时，只能每次读取一部分数据到缓冲区中，最后将全部数据拼接起来，得到
	//完整的快照数据
	m, err := dec.decodeLimit(uint64(1 << 63))				//读取时的异常处理
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		plog.Errorf(msg)
		http.Error(w, msg, http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	receivedBytes.WithLabelValues(from).Add(float64(m.Size()))

	if m.Type != raftpb.MsgSnap {							//检测读取到的消息类型，是否为MsgSnap，若不是，则直接返回异常消息
		plog.Errorf("unexpected raft message type %s on snapshot path", m.Type)
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	plog.Infof("receiving database snapshot [index:%d, from %s] ...", m.Snapshot.Metadata.Index, types.ID(m.From))
	// save incoming database snapshot.   使用Snapshotter将快照数据保存到本地文件中
	n, err := h.snapshotter.SaveDBFrom(r.Body, m.Snapshot.Metadata.Index)
	if err != nil {
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		plog.Error(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}
	receivedBytes.WithLabelValues(from).Add(float64(n))
	plog.Infof("received and saved database snapshot [index: %d, from: %s] successfully", m.Snapshot.Metadata.Index, types.ID(m.From))
	//调用Raft.Process方法将MsgSnap消息传递给底层的etcd-raft模块进行处理
	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			plog.Warningf(msg)
			http.Error(w, msg, http.StatusInternalServerError)
			snapshotReceiveFailures.WithLabelValues(from).Inc()
		}
		return
	}
	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)						//返回的状态码是204

	snapshotReceive.WithLabelValues(from).Inc()
	snapshotReceiveSeconds.WithLabelValues(from).Observe(time.Since(start).Seconds())
}

type streamHandler struct {
	tr         *Transport			//关联的rafthttp.Transport实例
	peerGetter peerGetter			//其接口中的Get方法会根据指定的节点ID获取对应的peer实例
	r          Raft					//底层的Raft实例
	id         types.ID				//当前节点的ID
	cid        types.ID				//当前集群的ID
}

func newStreamHandler(tr *Transport, pg peerGetter, r Raft, id, cid types.ID) http.Handler {
	return &streamHandler{
		tr:         tr,
		peerGetter: pg,
		r:          r,
		id:         id,
		cid:        cid,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//检测请求Method是否为GET，检测集群的ID
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	if err := checkClusterCompatibilityFromHeader(r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) {					//确定使用的协议版本
	case streamTypeMsgAppV2.endpoint():
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		plog.Debugf("ignored unexpected streaming request path %s", r.URL.Path)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)				//获取对端节点的ID
	from, err := types.IDFromString(fromStr)
	if err != nil {
		plog.Errorf("failed to parse from %s into ID (%v)", fromStr, err)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	if h.r.IsIDRemoved(uint64(from)) {
		plog.Warningf("rejected the stream from peer %s since it was removed", from)
		http.Error(w, "removed member", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from)						//根据对端节点ID获取对应的Peer实例
	if p == nil {
		// This may happen in following cases:
		// 1. user starts a remote peer that belongs to a different cluster
		// with the same cluster ID.
		// 2. local etcd falls behind of the cluster, and cannot recognize
		// the members that joined after its current progress.
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		plog.Errorf("failed to find member %s in cluster %s", from, h.cid)
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	wto := h.id.String()								//获取当前节点的ID
	if gto := r.Header.Get("X-Raft-To"); gto != wto {		//检测请求的目标节点是否为当前节点
		plog.Errorf("streaming request ignored (ID mismatch got %s want %s)", gto, wto)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)						//返回200状态码
	w.(http.Flusher).Flush()							//调用Flush方法将响应数据发送到对端节点

	c := newCloseNotifier()
	conn := &outgoingConn{								//创建outgoingConn实例
		t:       t,										//协议版本
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
	}
	//将outgoingConn实例与对应的streamWriter实例绑定
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

// checkClusterCompatibilityFromHeader checks the cluster compatibility of
// the local member from the given header.
// It checks whether the version of local member is compatible with
// the versions in the header, and whether the cluster ID of local member
// matches the one in the header.
func checkClusterCompatibilityFromHeader(header http.Header, cid types.ID) error {
	if err := checkVersionCompability(header.Get("X-Server-From"), serverVersion(header), minClusterVersion(header)); err != nil {
		plog.Errorf("request version incompatibility (%v)", err)
		return errIncompatibleVersion
	}
	if gcid := header.Get("X-Etcd-Cluster-ID"); gcid != cid.String() {
		plog.Errorf("request cluster ID mismatch (got %s want %s)", gcid, cid)
		return errClusterIDMismatch
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
