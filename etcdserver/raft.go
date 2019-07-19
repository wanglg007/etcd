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
	"encoding/json"
	"expvar"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/contention"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/coreos/pkg/capnslog"
)

const (
	// Number of entries for slow follower to catch-up after compacting
	// the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	numberOfCatchUpEntries = 5000

	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	raft.SetLogger(capnslog.NewPackageLogger("github.com/coreos/etcd", "raft"))
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		return raftStatus()
	}))
}

type RaftTimer interface {
	Index() uint64
	Term() uint64
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
}

type raftNode struct {
	// Cache of the latest raft index and raft term the server has seen.
	// These three unit64 fields must be the first elements to keep 64-bit
	// alignment for atomic access to the fields.
	index uint64						//当前节点中应用Entry记录的最大索引值
	term  uint64						//当前节点已应用Entry记录的最大任期号
	lead  uint64						//记录当前集群中Leader节点的ID值

	tickMu *sync.Mutex
	raftNodeConfig

	// a chan to send/receive snapshot
	// etcd-raft模块通过返回Ready实例与上层模块进行交互，其中Ready.Message字段记录了待发送的消息，其中可能会包含MsgSnap类型的消息，该类型消息中封装了需要
	// 发送到其他节点的快照数据。当raftNode收到MsgSnap消息之后，会将其写入MsgSnapC通道中，并等待上层模块进行发送。
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	// 在etcd-raft模块返回的Ready实例中，除了封装了待持久化的Entry记录和待持久化的快照数据，还封装了待应用的Entry记录。raftNode会经待应用的记录和快照数据封装成
	// apply实例。之后写入applyc通道等待上层模块处理。
	applyc chan apply

	// a chan to send out readState		封装只读请求相关的ReadState实例，其中的最后一项将会被写入ReadStateC通道中等待上层模块处理
	readStateC chan raft.ReadState

	// utility							该定时器就是逻辑时钟，每触发一次就会推进一次底层的选举计时器和心跳计时器
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	td *contention.TimeoutDetector		//检测发往同一节点的两次心跳消息是否超时，如果超时，则会打印相关警告信息

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	// to check if msg receiver is removed from cluster
	isIDRemoved func(id uint64) bool			//该函数用来检测指定节点是否已经被移出当前集群
	raft.Node
	raftStorage *raft.MemoryStorage				//用来保存持久化的Entry记录和快照数据
	storage     Storage
	heartbeat   time.Duration // for logging	逻辑时钟的刻度
	// transport specifies the transport to send and receive msgs to members.	通过网络将消息发送到集群中其他节点
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	r := &raftNode{
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),		//注意该通道的缓冲区大小
		applyc:     make(chan apply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {												//根据raftNodeConfig.heartbeat字段创建逻辑时钟，其时间刻度是heartbeat
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)							//每隔heartbeat触发一次
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}
// 该方法启动相关服务。方法中启动一个独立的后台goroutine，在该后台goroutine中完成了绝大部分与底层etcd-raft模块交互的功能。
// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	go func() {						//启动后台的goroutine提供服务
		defer r.onStop()
		islead := false					//刚启动时会将当前节点标识为follower

		for {
			select {
			case <-r.ticker.C:			//计时器到期被触发，调用Tick()方法推进选举计时器和心跳计时器
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {			//在SoftState中主要封装了当前集群的Leader信息和当前节点角色
					//检测集群的Leader节点是否发生变化，并记录相关监控信息
					newLeader := rd.SoftState.Lead != raft.None && atomic.LoadUint64(&r.lead) != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}
					//更新raftNode.lead字段，将其更新为新的Leader节点ID，注意，这里读取或是更新raftNode.lead字段都是原子操作
					atomic.StoreUint64(&r.lead, rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader			//记录当前节点是否为Leader节点
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					//调用raftReadyHandler中的updateLeadership()回调方法，其中会根据Leader节点是否发生变化，完成一些操作
					rh.updateLeadership(newLeader)
					r.td.Reset()										//重置全部探测器中的全部记录
				}

				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:	//将Ready.ReadStates中的最后一项写入readStateC通道中
					case <-time.After(internalTimeout):
						//如果上层应用一直没有读取写入ReadStateC通道中的ReadState实例，会导致本次写入阻塞，这里会等到1s，如果依然无法写入，则放弃写入并输出警告日志
						plog.Warningf("timed out sending read state")
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)								//创建notifyc通道
				//将Ready实例中的待应用Entry记录以及快照数据封装成apply实例，其中封装了notifyc通道，该通道用来协调当前goroutine和EtcdServer启动的后台
				//goroutine的执行
				ap := apply{
					entries:  rd.CommittedEntries,								//已提交、待用于的Entry记录
					snapshot: rd.Snapshot,										//待持久化的快照数据
					notifyc:  notifyc,
				}
				//更新EtcdServer中记录的已提交位置(EtcdServer.committedIndex字段)
				updateCommittedIndex(&ap, rh)

				select {
				case r.applyc <- ap:			//将apply实例写入applyc通道中，等待上层应用读取并进行处理
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them  如果当前节点处于Leader状态，则raftNode.start()方法
				// writing to their disks.                     会先调用raftNode.processMessage()方法对待发送的消息进行过滤，然后调用raftNode.transport.Send()
				// For more details, check raft thesis 10.2.1  方法完成消息的发送。
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.transport.Send(r.processMessages(rd.Messages))
				}
				// raftNode对Ready中待持久化的Eentry记录，以及快照数据的处理。
				// gofail: var raftBeforeSave struct{}
				// 通过raftNode.storage将Ready实例中携带的HardState信息和待持久化的Entry记录写入WAL日志文件中。
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					plog.Fatalf("raft save state and entries error: %v", err)
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))	//通过raftNode.storage将Ready实例中携带的快照数据保存到磁盘中
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						plog.Fatalf("raft save snapshot error: %v", err)
					}
					// etcdserver now claim the snapshot has been persisted onto the disk
					// EtcdServer中会启动后台goroutine读取applyc通道，并处理apply中封装快照数据。这里使用notifyc通道通知该后台goroutine，该apply实例中的快照数据
					// 已经被持久化到磁盘，后台goroutine可以开始应用该快照数据了。
					notifyc <- struct{}{}

					// gofail: var raftAfterSaveSnap struct{}
					r.raftStorage.ApplySnapshot(rd.Snapshot)			//将快照数据保存到MemoryStorage中
					plog.Infof("raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
					// gofail: var raftAfterApplySnap struct{}
				}

				r.raftStorage.Append(rd.Entries)						//将待持久化的Entry记录写入MemoryStorage中

				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					//处理Ready实例的过程基本结束，这里会通知EtcdServer启动的后台goroutine，检测是否生成快照
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.transport.Send(msgs)								//发送消息
				} else {
					// leader already processed 'MsgSnap' and signaled
					// 处理Ready实例的过程基本结束，这里会通知EtcdServer启动的后台goroutine，检测是否生成快照
					notifyc <- struct{}{}
				}
				//最后调用raft.node.Advance()方法，通知etcd-raft模块此次Ready实例已经处理完成，etcd-raft模块更新相应信息之后，可以继续返回Ready实例。
				r.Advance()
			case <-r.stopped:
				return					//当前节点已停止
			}
		}
	}()
}

func updateCommittedIndex(ap *apply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}
//该烦恼官方会对消息进行过滤，去除目标节点已被移出集群的消息，然后分别过滤MsgAppResp消息、MsgSnap消息和MsgHeartbeat消息。
func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {				//向后向前遍历全部待发送的消息
		if r.isIDRemoved(ms[i].To) {				//消息的目标节点已从集群中移除，将消息的目标节点ID设置为0，通过对etcd-raft模块的分析可知，其发送消息的过程中,
			//会忽略目标节点为0的消息
			ms[i].To = 0
		}
		//只会发送最后一条MsgAppResp消息，通过etcd-raft分析可知，没有必要同时发送多条MsgAppResp消息
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {				//对MsgSnap消息的处理
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:					//将MsgSnap消息写入MsgSnapC通道中
			default:									//如果msgSnapC通道的缓冲区满了，则放弃此次快照的发送
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0								//将目标节点设置为0，则raftNode.transport后续不会发送该消息
		}
		if ms[i].Type == raftpb.MsgHeartbeat {			//对MsgHeartbeat类型的消息
			//通过TimeoutDetector进行检测，检测发往目标节点的心跳消息间隔是否过大
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {									//输出警告日志，表示当前节点可能已经过载了。
				// TODO: limit request rate.
				plog.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v, to %x)", r.heartbeat, exceed, ms[i].To)
				plog.Warningf("server is likely overloaded")
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) apply() chan apply {
	return r.applyc
}

func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		plog.Panicf("raft close storage error: %v", err)
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func startNode(cfg ServerConfig, cl *membership.RaftCluster, ids []types.ID) (id types.ID, n raft.Node, s *raft.MemoryStorage, w *wal.WAL) {
	var err error
	member := cl.MemberByName(cfg.Name)								//根据当前节点的名称，在RaftCluster中查找对应的Member实例
	metadata := pbutil.MustMarshal(									//将节点的id和当前集群的id封装后进行序列化
		&pb.Metadata{
			NodeID:    uint64(member.ID),							//节点的id
			ClusterID: uint64(cl.ID()),								//集群的id
		},
	)
	if w, err = wal.Create(cfg.WALDir(), metadata); err != nil {	//创建WAL日志文件，并将上述元数据信息作为第一条日志记录写入WAL日志文件中
		plog.Panicf("create wal error: %v", err)
	}
	peers := make([]raft.Peer, len(ids))							//为集群中每个节点，创建对应的Peer实例
	for i, id := range ids {
		ctx, err := json.Marshal((*cl).Member(id))					//直接将Member实例序列化
		if err != nil {
			plog.Panicf("marshal member should never fail: %v", err)
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}			//记录了节点id和Member实例序列化后的数据
	}
	id = member.ID													//当前节点的ID
	plog.Infof("starting member %s in cluster %s", id, cl.ID())
	s = raft.NewMemoryStorage()										//新建MemoryStorage实例
	c := &raft.Config{												//初始化Node实例时使用的相关配置
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
	}

	n = raft.StartNode(c, peers)									//创建raft.Node
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, n, s, w
}
//该函数会根据配置信息和加载的快照数据，重启etcd-raft模块的Node实例。
func restartNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.WALDir(), walsnap)			//根据快照的元数据，查找合适的WAL日志并完成WAL日志的回放

	plog.Infof("restarting member %s in cluster %s at commit index %d", id, cid, st.Commit)
	cl := membership.NewCluster("")
	cl.SetID(cid)
	s := raft.NewMemoryStorage()									//创建MemoryStorage实例
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)									//将快照数据记录到MemoryStorage实例中
	}
	s.SetHardState(st)												//根据WAL日志文件回放的结果设置HardState
	s.Append(ents)													//向MemoryStorage实例中追加快照数据之后的Entry记录
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
	}
	//根据上面的raft.Config，重建raft.Node实例
	n := raft.RestartNode(c)
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, cl, n, s, w
}

func restartAsStandaloneNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.WALDir(), walsnap)

	// discard the previously uncommitted entries
	for i, ent := range ents {
		if ent.Index > st.Commit {
			plog.Infof("discarding %d uncommitted WAL entries ", len(ents)-i)
			ents = ents[:i]
			break
		}
	}

	// force append the configuration change entries
	toAppEnts := createConfigChangeEnts(getIDs(snapshot, ents), uint64(id), st.Term, st.Commit)
	ents = append(ents, toAppEnts...)

	// force commit newly appended entries
	err := w.Save(raftpb.HardState{}, toAppEnts)
	if err != nil {
		plog.Fatalf("%v", err)
	}
	if len(ents) != 0 {
		st.Commit = ents[len(ents)-1].Index
	}

	plog.Printf("forcing restart of member %s in cluster %s at commit index %d", id, cid, st.Commit)
	cl := membership.NewCluster("")
	cl.SetID(cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
	}
	n := raft.RestartNode(c)
	raftStatus = n.Status
	return id, cl, n, s, w
}

// getIDs returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain two kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
func getIDs(snap *raftpb.Snapshot, ents []raftpb.Entry) []uint64 {
	ids := make(map[uint64]bool)
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Nodes {
			ids[id] = true
		}
	}
	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeRemoveNode:
			delete(ids, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
			// do nothing
		default:
			plog.Panicf("ConfChange Type should be either ConfChangeAddNode or ConfChangeRemoveNode!")
		}
	}
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids)
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(ids []uint64, self uint64, term, index uint64) []raftpb.Entry {
	ents := make([]raftpb.Entry, 0)
	next := index + 1
	found := false
	for _, id := range ids {
		if id == self {
			found = true
			continue
		}
		cc := &raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}
	if !found {
		m := membership.Member{
			ID:             types.ID(self),
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
		}
		ctx, err := json.Marshal(m)
		if err != nil {
			plog.Panicf("marshal member should never fail: %v", err)
		}
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  self,
			Context: ctx,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
	}
	return ents
}
