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

package raft

import (
	"context"
	"errors"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.    当前集群的Leader节点ID
	RaftState StateType																  //当前节点的角色
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally   该字段记录了当前节点中等待处理的只读请求
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE    该字段中的Entry记录是从unstable中读取出来的，上层模块会将其保存到Storage中
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.	 待持久化的快照数据
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a           已提交、待应用的Entry记录，这些Entry记录之前已经保存到了Storage中
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft   该字段中保存了当前节点中等待发送到集群其他节点的Message消息
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}
//该方法检测Ready实例中各个字段是否为空，从而决定此次创建的Ready实例是否有必要交给上层模块进行处理。
func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||					//检测raft状态是否发生变化
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||								//检测是否有新的快照数据 || 检测是否有待持久化的Entry记录
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0	//检测是否有待应用的Entry记录 || 检测是否有待发送的消息 || 检测是否有处理的只读请求
}
// 结构体Node表示集群中的一个节点，它是在结构体raft之上的一层封装，对外提供相对简单的API接口
// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election  Tick方法用来推进逻辑时钟的指针，从而推进
	// timeouts and heartbeat timeouts are in units of ticks.                              上述的两个计时器
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	// 当选举计时器超时之后，会调用该方法会将当前节点切换成Candidate状态(或是PerCandidate状态)，底层就是通过发送MsgHup消息实现的
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log.
	// 接收到Client发来的写请求时，Node实例会调用该方法进行处理，底层就是通过发送MsgProp消息实现的
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes config change.
	// At most one ConfChange can be in the process of going through consensus.
	// Application needs to call ApplyConfChange when applying EntryConfChange type entry.
	// Client除了会发送读写请求，还会发送修改集群配置的请求(例如新增集群中的节点)，这种请求Node实例会调用该方法进行处理。底层就是通过
	// 发送MsgProp消息实现的，只不过其中记录的Entry记录是EntryConfChange类型
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	// 当前节点收到其他节点的消息时，会通过该方法将消息交给底层封装的raft实例进行处理
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.                      Ready()方法返回的是一个Channel，通过该Channel返回的Ready实例中封装了底层
	// Users of the Node must call Advance after retrieving the state returned by Ready.          raft实例的相关状态数据，例如，需要发送到其他节点的消息、交给上层模块的Entry
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries  记录，等待。
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.    当上层模块处理完从上述Channel中返回的Ready实例之后，需要调用Advance()通知底层
	// It prepares the node to return the next available Ready.                                   的etcd-raft模块返回新的Ready实例
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()
	// ApplyConfChange applies config change to the local node.                                   在收到集群配置请求时，会通过调用该方法进行处理
	// Returns an opaque ConfState protobuf which must be recorded
	// in snapshots. Will never return nil; it returns a pointer only
	// to match MemoryStorage.Compact.
	ApplyConfChange(cc pb.ConfChange) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.				  该方法用于Leader节点的转移
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.					  该方法用于处理只读请求
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.								  该方法返回当前节点的状态运行状态
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.				  通过该方法通知底层的raft实例，当前节点无法与指定的节点进行通信
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot.									  该方法用于通知底层的raft实例上次发送快照的结果
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.										  关闭当前节点
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}
// 当集群中的节点初次启动时会通过该方法启动创建对应的node实例和底层的raft实例。c参数:Peer封装了节点的ID,peers参数:记录当前集群中全部节点的ID
// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
func StartNode(c *Config, peers []Peer) Node {
	r := newRaft(c)								//根据Config配置信息创建raft实例
	// become the follower at term 1 and apply initial configuration
	// entries of term 1
	r.becomeFollower(1, None)				//切换成Follower状态，因为节点初次启动，所以任期号为1
	for _, peer := range peers {
		//根据传递的节点列表，创建对应的ConfChange实例，其Type是ConfChangeAddNode，表示添加指定节点
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		d, err := cc.Marshal()					//序列化ConfChange记录
		if err != nil {
			panic("unexpected marshal error")
		}
		//将ConfChange记录序列化后的数据封装成EntryConfChange类型的Entry记录
		e := pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: r.raftLog.lastIndex() + 1, Data: d}
		r.raftLog.append(e)						//将EntryConfChange记录追加到raftLog中
	}
	// Mark these initial entries as committed.
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	r.raftLog.committed = r.raftLog.lastIndex()//直接修改raftLog.committed值，提交上述EntryConfChange记录
	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	for _, peer := range peers {				//为节点列表中的每个节点都创建对应的Progress实例
		r.addNode(peer.ID)						//添加节点
	}

	n := newNode()								//初始化node实例
	n.logger = c.Logger
	go n.run(r)									//启动一个goroutine，其中会根据底层raft的状态及上层模块传递的数据，协调处理node中各种通道的数据
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.       当集群中的节点重新启动时，就不是通过StartNode函数创建node实例，而是通过调用
// The current membership of the cluster will be restored from the Storage.     RestartNode函数，该函数与StartNode函数实现非常类似，其中最主要的区别就是不会
// If the caller has an existing state machine, pass in the last log index that 根据Config中的配置信息调用raft.addNode方法添加节点，因为这些信息会从Storage中恢复。
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	r := newRaft(c)				//创建底层封装的raft实例，该过程与StartNode函数相同

	n := newNode()				//创建node实例
	n.logger = c.Logger
	go n.run(r)					//启动一个goroutine，其中会根据底层raft的状态及上层模块传递的数据，协调处理node中各种通道的数据
	return &n
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan pb.Message			//该通过用于接收MsgProp类型的消息
	recvc      chan pb.Message			//除MsgProp外的其他类型的消息都是由该通道接收的
	confc      chan pb.ConfChange		//当节点收到EntryConfChange类型的Entry记录时，会转换成ConfChange，并写入该通道中等待处理。
	confstatec chan pb.ConfState		//在ConfState中封装了当前集群中所有节点的ID，该通道用于向上层模块返回ConfState实例
	readyc     chan Ready				//该通过用于向上层模块返回Ready实例
	advancec   chan struct{}			//当上层模块处理完通过上述readyc通道获取到的Ready实例之后，会通过node.Advance()方法向该通道写入信号，从而通知底层raft实例
	tickc      chan struct{}			//用来接收逻辑时钟发出的信号，之后会根据节点当前节点的角色推进选举计时器和心跳计时器
	done       chan struct{}			//当检测到done通道关闭后，在其上阻塞的goroutine会继续执行，并进行相应的关闭操作
	stop       chan struct{}			//当node.Stop()方法被调用时，会向该通道发送信号。有另一个goroutine会尝试读取该通道中的内容，当读取到信息之后，会关闭done通道
	status     chan chan Status		//

	logger Logger
}

func newNode() node {
	return node{
		propc:      make(chan pb.Message),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChange),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),			//带缓冲的Channel
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}
//该方法会处理node结构体中封装的全部通道，它也是node的核心。
func (n *node) run(r *raft) {
	//相关变量的含义及初始化
	var propc chan pb.Message							//指向node.propc通道
	var readyc chan Ready								//指向node.readyc通道
	var advancec chan struct{}						//指向node.advancec通道
	var prevLastUnstablei, prevLastUnstablet uint64		//用于记录unstable中最后一条Entry记录的索引值和Term值
	var havePrevLastUnstablei bool
	var prevSnapi uint64								//用于记录快照中最后一条记录的索引值
	var rd Ready

	lead := None										//用于记录当前Leader节点
	prevSoftSt := r.softState()							//用于记录SoftState(当前Leader节点ID和当前节点的角色)
	prevHardSt := emptyState							//用于记录HardState

	for {												//在该循环中完成了对所有相关Channel的处理
		//创建Ready实例并进行检测，检测当前集群的Leader节点并设置propc
		if advancec != nil {
			readyc = nil								//上层模块还在处理上次从readyc通道返回的Ready实例，所以不能继续向readyc中写入数据
		} else {
			rd = newReady(r, prevSoftSt, prevHardSt)	//创建Ready实例
			if rd.containsUpdates() {					//根据Ready决定，是否存在需要交给上层模块处理的数据
				readyc = n.readyc
			} else {
				readyc = nil							//此次创建的Ready实例中各个字段都为空，无需交给上层模块处理
			}
		}

		if lead != r.lead {								//检测当前的Leader节点是否发生变化
			if r.hasLeader() {							//如果当前节点无法确定集群中的Leader节点，则清空propc，此次循环不再处理MsgPropc消息
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead								//更新Leader
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-propc:								//读取propc通道，获取MsgPropc消息，并交给raft.Step()方法处理
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:							//读取node.recvc通道，获取消息(非MsgPropc类型)，并交给raft.Step()方法进行处理
			// filter out response message from unknown From.  	如果是来自未知节点的响应消息(例如,MsgHeartbeatResp类型消息)则会被过滤
			if pr := r.getProgress(m.From); pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m) // raft never returns an error
			}
		case cc := <-n.confc:							//读取node.confc通道，获取ConfChange实例，下面根据ConfChange中的类型进行分类处理
			if cc.NodeID == None {						//ConfChange中未指定待处理的节点ID
				r.resetPendingConf()					//重置pengdingConf字段为false，表示不存在待处理的节点变更请求
				select {								//将当前集群中的节点信息封装成ConfState实例并写入confstatec通道中
				case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:
				case <-n.done:
				}
				break
			}
			switch cc.Type {
			case pb.ConfChangeAddNode:					//添加节点
				r.addNode(cc.NodeID)
			case pb.ConfChangeAddLearnerNode:
				r.addLearner(cc.NodeID)
			case pb.ConfChangeRemoveNode:				//删除节点
				// block incoming proposal when local node is
				// removed
				if cc.NodeID == r.id {
					propc = nil							//在删除当前节点之前，首先会清空propc变量，保证后续不再处理MsgPropc消息
				}
				r.removeNode(cc.NodeID)
			case pb.ConfChangeUpdateNode:				//更新节点
				r.resetPendingConf()					//重置pendingConf字段为false
			default:
				panic("unexpected conf type")		//未知的ConfChange类型
			}
			select {									//将当前集群中节点的信息封装成ConfState实例并写入confStatec通道中，上层模块会读取该通道，并从中获取当前集群的节点信息
			case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:
			case <-n.done:
			}
		case <-n.tickc:							//逻辑时钟每推进一次，就会向tickc通道写入一个信号
			r.tick()
		case readyc <- rd:						//将前面创建的Ready实例写入node.readyc通道中，等待上层模块读取
			if rd.SoftState != nil {
				prevSoftSt = rd.SoftState		//prevSoftSt记录此次返回的Ready实例的SoftState状态
			}
			if len(rd.Entries) > 0 {			//记录此次返回的Ready实例是否包含待持久化的Entry，并记录最后一条记录的索引值和Term值
				prevLastUnstablei = rd.Entries[len(rd.Entries)-1].Index
				prevLastUnstablet = rd.Entries[len(rd.Entries)-1].Term
				havePrevLastUnstablei = true
			}
			if !IsEmptyHardState(rd.HardState) {
				prevHardSt = rd.HardState		//prevHardSt记录此次返回的Ready实例的HardState状态
			}
			if !IsEmptySnap(rd.Snapshot) {		//prevSnapi记录了此次返回的Ready实例中快照的元数据
				prevSnapi = rd.Snapshot.Metadata.Index
			}

			r.msgs = nil						//清空raft.msgs和raft.readStates
			r.readStates = nil
			advancec = n.advancec				//将advancec指向node.advancec通道，这样在下次for循环时，就无法继续向上层模块返回Ready实例。
		case <-advancec:						//上层模块处理完Ready实例之后，会向advance通道写入信号
			if prevHardSt.Commit != 0 {			//更新raftLog.applied字段(即已应用的记录位置)
				r.raftLog.appliedTo(prevHardSt.Commit)
			}
			if havePrevLastUnstablei {
				//Ready.Entries中的记录已经被持久化了，这里会将unstable中的对应记录清理掉，并更新其offset，也可能缩小unstable底层保存Entry记录的数组
				r.raftLog.stableTo(prevLastUnstablei, prevLastUnstablet)
				havePrevLastUnstablei = false
			}
			r.raftLog.stableSnapTo(prevSnapi)	//Ready中封装的快照数据已经被持久化，这里会清空unstable中记录的快照数据
			advancec = nil						//清空advancec，下次for循环处理的过程中，就能向readyc通道写入Ready了。
		case c := <-n.status:					//
			c <- getStatus(r)					//创建Status实例
		case <-n.stop:							//当从stop通道中读取到信号时，会将done通道关闭，阻塞在其上的goroutine可以继续执行
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.logger.Warningf("A tick missed to fire. Node blocks too long!")
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}
//该方法主要处理从其他节点收到的网络消息，而不是本地消息。
func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) step(ctx context.Context, m pb.Message) error {
	ch := n.recvc             	//根据传入的消息类型，选择合适的通道
	if m.Type == pb.MsgProp {
		ch = n.propc
	}

	select {
	case ch <- m:				//将消息写入recvc或propc通道中
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:          //向advancec通道写入信号
	case <-n.done:                           //当前node已关闭
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc:
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)				//创建一个用于传递Status实例的通道
	select {
	case n.status <- c:					//将该通道写入到node.status通道中
		return <-c
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure            //用于设置MsgSnapStatus消息的Reject

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}
//该函数会读取底层raft实例中的各项数据及相关状态，并最终封装成Ready实例，该Ready实例最终会返回给上层模块。
func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),		//获取raftLog中unstable部分存储的Entry记录，这些Entry记录会交给上层模块进行持久化
		CommittedEntries: r.raftLog.nextEnts(),				//获取已提交但未应用的Entry记录，即raftLog中applied-committed直接的所有记录
		Messages:         r.msgs,							//获取待发送消息
	}
	//检测两次创建Ready实例之间，raft实例状态是否发生变化，如果无变化，则将Ready实例相关字段设置为nil，表示无需上层模块处理
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	//检测unstable中是否记录了新的快照数据，如果有，则将其封装到Ready实例中，交给上层模块进行处理
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	//在介绍只读请求的处理时，raft最终会将能响应的请求信息封装成ReadState并记录到readStates中，这里会检测
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(rd.HardState, prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
