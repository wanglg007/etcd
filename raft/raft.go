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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}
// Config结构体主要用于配置参数的传递，在创建raft实例时需要的参数会通过Config实例传递进去;
// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.							当前节点的ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It		记录集群中所有节点的ID
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// learners contains the IDs of all leaner nodes (including self if the local node is a leaner) in the raft cluster.
	// learners only receives entries from the leader node. It does not vote or promote itself.
	learners []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int			//用于初始化raft.electionTimeout，即逻辑时钟连续推进多少次后，就会触发Follower节点的状态切换及新一轮的Leader选举
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int			//用于初始化raft.heartbeatTimeout，即逻辑时钟连续推进多少次后，就会触发Leader节点发送心跳消息

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage				//当前节点保存raft日志记录使用的存储
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64				//当前已经应用的记录位置(已应用的最后一条Entry记录的索引值)，该值在节点重启时需要设置，否则会重新应用已经应用过Entry记录

	// MaxSizePerMsg limits the max size of each append message. Smaller value       用于初始化raft.maxMsgSize字段，每条消息的最大字节数，如果
	// lowers the raft recovery cost(initial probing and message lost during normal	 是math.MaxUint64则没有上限，如果是0则表示每条消息最多携带
	// operation). On the other side, it might affect the throughput during normal   一条Entry。
	// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	// message.
	MaxSizePerMsg uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during     用于初始化raft.maxInflight，即已经发送出去且未收到响应
	// optimistic replication phase. The application transportation layer usually	 的最大消息个数
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader		是否开启CheckQuorum模式，用于初始化raft.checkQuorum字段
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section		是否开启PreVote模式，用于初始化raft.preVote字段
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.				与只读请求的处理相关
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an innaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}
//该结构封装了当前节点所有的核心数据
type raft struct {
	id uint64				//当前节点在集群中的ID

	Term uint64				//当前任期号。如果Message的Term字段为0，则表示该消息是本地消息。例如后面的MsgHup、MsgProp、MsgReadIndex等都属于本地消息
	Vote uint64				//当前任期中当前节点将选票投给了哪个节点，未投票时，该字段为None

	readStates []ReadState	//与只读请求相关

	// the log
	raftLog *raftLog		//在Raft协议中的每个节点都会记录本地Log
	//若处于该状态的消息超过maxInflight这个阈值，则暂停当前节点的发送。防止集群中的某个节点不断发送消息，引起网络阻塞或压垮其他节点。
	maxInflight int			//对于当前节点来说，已经发送出去但未收到响应的消息个数上限。
	maxMsgSize  uint64		//单条消息的最大字节数
	//Leader节点会记录集群中其他节点的日志复制情况。在etcd-raft模块中，每个Follower节点对应的NextIndex和MatchIndex值都封装在Progress实例中
	prs         map[uint64]*Progress
	learnerPrs  map[uint64]*Progress

	state StateType			//当前节点在集群中的角色，可选值为StateFollower、StateCandidate、StateLeader、StatePreCandidate四种状态

	// isLearner is true if the local raft node is a learner.
	isLearner bool

	votes map[uint64]bool	//在选举过程，若当前节点收到来自某个节点的投票，则会将votes中对应的值设置为true，通过统计votes这个map,可以确定当前节点收到的投票是否超过半数

	msgs []pb.Message		//缓存当前节点等待发送的消息

	// the leader id
	lead uint64				//当前集群中Leader节点的ID
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64	//用于集群中Leader节点的转移，它记录了此次Leader角色转移的目标节点的ID
	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool

	readOnly *readOnly		//与只读请求有个

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int		//选举计时器的指针，其单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会增加1

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int	//心跳计数器的指针，其单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段就会增加1
	//CheckQuorum机制：每隔一段时间，Leader节点会尝试连接集群中的其他节点，如果发现自己可以连接到节点个数没有超过半数，则主动切换成Follower状态。
	checkQuorum bool
	//PreVote:当Follower节点准备发起一次选举之前，会连接集群中的其他节点，并询问它们是否愿意参与选举。若集群中的其他节点能够正常收到Leader节点的
	//心跳消息，则会拒绝参与选举，反之则参与选举。当在PreVote过程中，有超过半数的节点响应并参与新一轮选举，则可以发起新一轮的选举。
	preVote     bool

	heartbeatTimeout int	//心跳超时时间，当heartbeatElapsed字段值达到该值时，就会触发Leader节点发送一条心跳消息
	electionTimeout  int	//选举超时时间，当electionElapsed字段值达到该值时，就会触发新一轮的选举
	// randomizedElectionTimeout is a random number between       该字段是electiontimeout ~ 2*electiontimeout-1之间的随机值，也是选举计数器的
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset  上限，当electionElapsed超过该值时即为超时
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	disableProposalForwarding bool
	//当前节点推进逻辑时钟的函数。如果当前节点是Leader，则指向raft.tickHeartbeat函数，如果当前节点是Follower或是Candidate，则指向raft.tickElection函数
	tick func()
	//当前节点收到消息时的处理函数。若是Leader节点，则该字段指向stepLeader函数，如果是Follower节点，则该字段指向stepFollower函数，如果处于preVote阶段
	//的节点或是Candidate节点，则该字段指向stepCandidate函数
	step stepFunc

	logger Logger
}
//该方法会根据传入的Config实例中携带的参数创建raft实例并初始化raft使用到的相关组件。
func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {		//检测参数Config中各字段的合法性
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)		//创建raftLog实例，用于记录Entry记录
	hs, cs, err := c.Storage.InitialState()		//获取raftLog.storage的初始状态(HardState和ConfState)，Storage的初始状态是通过本地Entry记录回放得到的
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers							//根据快照中记录的集群信息和Config的配置信息初始化集群的节点信息
	learners := c.learners
	if len(cs.Nodes) > 0 || len(cs.Learners) > 0 {
		if len(peers) > 0 || len(learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = cs.Nodes
		learners = cs.Learners
	}
	//创建raft实例
	r := &raft{
		id:                        c.ID,							//当前节点的ID
		lead:                      None,							//当前集群中的Leader节点的ID，初始化时先被设置成0
		isLearner:                 false,
		raftLog:                   raftlog,							//负责管理Entry记录的raftLog实例
		maxMsgSize:                c.MaxSizePerMsg,					//每条消息的最大字节数，如果是math.MaxUint64则没有上限，如果是0则表示每条消息最多携带一条Entry
		maxInflight:               c.MaxInflightMsgs,				//已经发送出去且未收到响应的最大消息个数
		prs:                       make(map[uint64]*Progress),
		learnerPrs:                make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,					//选举超时时间
		heartbeatTimeout:          c.HeartbeatTick,					//心跳超时时间
		logger:                    c.Logger,						//普通日志输出
		checkQuorum:               c.CheckQuorum,					//是否开始CheckQuorum模式
		preVote:                   c.PreVote,						//是否开始PreVote模式
		readOnly:                  newReadOnly(c.ReadOnlyOption),	//只读请求的相关配置
		disableProposalForwarding: c.DisableProposalForwarding,
	}
	//初始化raft.prs字段，这里会根据集群中节点的ID，为每个节点初始化Progress实例，在Progress中维护了对应节点的NextIndex值和MatchIndex值，以及
	//一些其他的Follower节点信息。注意：只有Leader节点的raft.prs字段是有效的。
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}
	for _, p := range learners {
		if _, ok := r.prs[p]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.learnerPrs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight), IsLearner: true}
		if r.id == p {
			r.isLearner = true
		}
	}
	//根据从Storage中获取的HardState，初始化raftLog.committed字段，以及raft.Term和Vote字段。
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	//如果Config中配置了Applied，则将raftLog.applied字段重置为指定的Applied值。上层模块自己的控制正确的已应用位置时使用该配置。
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)				//当前节点切换成Follower状态

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int { return len(r.prs)/2 + 1 }

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs)+len(r.learnerPrs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	for id := range r.learnerPrs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}
//该方法会在消息发送之前对不同类型的消息进行合法性检测，然后将待发送的消息追加到raft.msg字段中。
// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	m.From = r.id					//设置消息的发送节点ID，即当前节点ID
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {			//对MsgVote和MsgPreVote消息的Term字段进行检测
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {			//对其他类型消息的Term字段值进行设置
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex        除了MsgProp和MsgReadIndex两类消息(这两类消息的Term值为0，即为本地消息)之外，其他
		// proposals are a way to forward to the leader and   类型消息的Term字段值在这里统一设置
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)		//将消息添加到r.msgs切片中等待发送
}

func (r *raft) getProgress(id uint64) *Progress {
	if pr, ok := r.prs[id]; ok {
		return pr
	}

	return r.learnerPrs[id]
}
// 该方法主要负责向指定的目标节点发送MsgAPP消息(或MsgSnap消息)，在消息发送之前会检测当前节点的状态，然后查找待发送的Entry记录并封装成MsgAPP消息，
// 之后根据对应节点的Progress.State值决定发送消息之后的操作。
// sendAppend sends RPC, with entries to the given peer.
func (r *raft) sendAppend(to uint64) {
	pr := r.getProgress(to)
	if pr.IsPaused() {				//检测当前节点是否可以向目标节点发送消息
		return
	}
	m := pb.Message{}				//创建待发送的消息(Message实例)
	m.To = to						//设置目标节点的ID
	//根据当前Leader节点记录的Next查找发往指定节点的Entry记录(ents)及Next索引对应的记录的Term值
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	//上述两次raftLog查找出现异常时，就会形成MsgSnap消息，将快照数据发送到指定节点
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries   若上述raftLog的查找出现异常，则会尝试发送MsgSnap消息
		if !pr.RecentActive {		//从当前的Leader节点的角度来看，目标Follower节点已经不再存活
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}

		m.Type = pb.MsgSnap			//将消息类型设置为MsgSnap，为后续发送快照数据做准备
		snapshot, err := r.raftLog.snapshot()		//获取快照数据
		if err != nil {				//异常检测，如果获取快照数据出现异常，则终止整个程序
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot		//设置MsgSnap消息的Snapshot字段
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term		//获取快照的相关信息
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)	//将目标Follower节点对应的Progress切换成ProgressStateSnapshot状态，其中会用Progress.PendingSnapshot字段记录快照数据的记录
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {						//发送MsgAPP消息
		m.Type = pb.MsgApp					//设置消息类型
		m.Index = pr.Next - 1				//设置MsgAPP消息的Index字段
		m.LogTerm = term					//设置MsgAPP消息的LogTerm字段
		m.Entries = ents					//设置消息携带的Entry记录集合
		m.Commit = r.raftLog.committed		//设置消息的Commit字段，即当前节点的raftLog中最后一条已提交的记录索引值
		if n := len(m.Entries); n != 0 {
			switch pr.State {				//根据目标节点对应的Progress.State状态决定其发送消息后的行为
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:	//ProgressStateReplicate状态下的处理
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)	//更新目标节点对应的Next值
				pr.ins.add(last)			//记录已发送但是未收到响应的消息
			case ProgressStateProbe:		//ProgressStateProbe状态下的处理
				pr.pause()					//消息发送后，就将Progress.Paused字段设置为true，暂停后续消息的发送
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	//发送前面创建的消息
	r.send(m)
}

// sendHeartbeat sends an empty MsgApp
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).           注意MsgHeartbeat消息中Commit字段的设置，这主要是因为在发送该MsgHeartbeat消息时，
	// When the leader sends out heartbeat message,					Follower节点并不一定已经收到了全部已提交的Entry记录
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.getProgress(to).Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

func (r *raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.prs {
		f(id, pr)
	}

	for id, pr := range r.learnerPrs {
		f(id, pr)
	}
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date  该方法主要负责向集群中的其他节点发送MsgAPP消息(或MsgSnap消息)
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {		//过滤当前节点本身，只向集群中其他节点发送消息
			return
		}

		r.sendAppend(id)	//向指定节点发送消息
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {				//过滤当前节点自身
			return
		}
		r.sendHeartbeat(id, ctx)	//向指定的节点发送MsgBeat消息
	})
}
// 若该Entry记录已经复制到了半数以上的节点中，则在该方法中尝试将其提交。
// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	// TODO(bmizerany): optimize.. Currently naive
	mis := make(uint64Slice, 0, len(r.prs))		//将集群中所有节点对应的Progress.Match字段复制到mis切片中
	for _, p := range r.prs {
		mis = append(mis, p.Match)
	}
	sort.Sort(sort.Reverse(mis))				//对这些Match值进行排序
	//raft.quorum()方法返回值是集群节点的半数+1，举例：如果节点数量为5，r.quorum()-1=2，则可以找到mis切片中下标为2的节点对应的Match值。该值
	//之前的Entry记录都是可以提交的，因为节点0、1/2三个节点(超过半数)已经复制了该记录
	mci := mis[r.quorum()-1]
	return r.raftLog.maybeCommit(mci, r.Term)	//更新raftLog.commited字段，完成提交
}

func (r *raft) reset(term uint64) {			//该方法会重置raft实例的多个字段
	if r.Term != term {
		r.Term = term						//重置Term字段
		r.Vote = None						//重置Vote字段
	}
	r.lead = None							//清空lead字段
	//重置选举计时器和心跳计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()		//重置选举计时器的过期时间(随机值)

	r.abortLeaderTransfer()					//清空leadTransferee

	r.votes = make(map[uint64]bool)			//重置votes字段
	//重置prs，其中每个Progress中的Next设置为raftLog.lastIndex
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight), IsLearner: pr.IsLearner}
		if id == r.id {						//将当期节点对应的prs.Match设置为lastIndex
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConf = false					//清空pendingConf字段
	r.readOnly = newReadOnly(r.readOnly.option)			//只读请求相关配置
}
//主要步骤如下：(1)设置待追加的Entry记录的Term值和Index值；(2)向当前节点的raftLog中追加Entry记录；(3)更新当前节点对应的Progress实例；(4)尝试提交
//Entry记录，即修改raftLog.committed字段的值；
func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()				//获取raftLog中最后一条记录的索引值
	for i := range es {					//更新待追加记录的Term值和索引值
		es[i].Term = r.Term					//Entry记录的Term指定为当前Leader节点的任期号
		es[i].Index = li + 1 + uint64(i)	//为日志记录指定Index
	}
	r.raftLog.append(es...)					//向raftLog中追加记录
	//更新当前节点对应的Progress，主要是更新Next和Match
	r.getProgress(r.id).maybeUpdate(r.raftLog.lastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()							//尝试提交Entry记录
}
// 当节点变成Follower状态之后，会周期性的调研raft.tickElection方法推进electionElapsed并检测是否超时
// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++									//递增electionElapsed
	//promotable方法会检测prs字段中是否还存在当期节点对应的Progress实例，这是为了检测当期节点是否被从集群中移除；pastElectionTimeout方法检测当期
	//的选举计时器是否超时(r.electionElapsed>=r.randomizedElectionTimeout)
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0							//重置electionElapsed
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})	//发起选举
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++								//递增心跳计时器
	r.electionElapsed++									//递增选举计时器

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0							//重置选举计时器，Leader节点不会主动发起选举
		if r.checkQuorum {								//进行多数检测
			//当选举计时器超过electionTimeout时，会触发一次checkQuorum操作。该操作并不会发送网络消息，它只是检测当前节点是否与集群中的大部分节点连通
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		//选举计时器处于elatcionTImeout~randomizedElactionTimeout时间段时，不能进行Leader节点的转移
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()						//情况raft.leadTransferee字段，放弃转移
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {		//心跳计时器超时
		r.heartbeatElapsed = 0							//重置心跳计时器
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})//发送MsgBeat消息
	}
}
//该方法将节点切换为Follower状态
func (r *raft) becomeFollower(term uint64, lead uint64) {
	//将step字段设置为stepFollower，stepFollower函数中封装了Follower节点处理消息的行为
	r.step = stepFollower
	r.reset(term)				//重置raft实例Term、Vote等字段
	r.tick = r.tickElection		//将tick字段设置成tickElection函数
	r.lead = lead				//设置当前集群的Leader节点
	r.state = StateFollower		//设置当前节点的角色
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}
//当节点可以连接到集群中半数以上的节点时，会调用该方法切换到Candidate状态。
func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {	//检测当前节点的状态，禁止直接从Leader状态切换到PreCandidate状态
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate		//将step字段设置成stepCandidate，stepCandidate函数中封装了Candidate节点处理消息的行为
	r.reset(r.Term + 1)			//重置raft实例Term、Vote等字段，这里的Term已经递增
	r.tick = r.tickElection		//raft.tickElection
	r.Vote = r.id				//在此次选举中，Candidate节点会将选票投给自己
	r.state = StateCandidate	//修改当前节点的角色[注意：切换成Candidate状态之后，raft.Leader字段为None，这与其他状态不一样]
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}
//若当前集群开启了PreVote模式，当Follower节点的选举计时器超时时，会先调用该方法切换到PreCandidate状态
func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {			//检测当前节点的状态，禁止直接从Leader状态切换为PreCandidate状态
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,          将step字段设置为stepCandidate，stepCandidate函数中封装了
	// but doesn't change anything else. In particular it does not increase    PreCandidate节点处理消息的行为
	// r.Term or change r.Vote.
	r.step = stepCandidate
	r.votes = make(map[uint64]bool)
	r.tick = r.tickElection				//
	r.state = StatePreCandidate			//修改当前节点的角色
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}
//当Candidate节点得到集群中半数以上节点的选票时，会调用该方法切换成Leader状态。
func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {		//检测当前节点的状态，进制从Follower状态切换成Leader状态
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader					//将step字段设置为stepLeader，stepLeader函数封装了Leader节点处理消息的行为
	r.reset(r.Term)						//重置raft实例Term、Vote等字段
	r.tick = r.tickHeartbeat			//将tick字段设置成tickHeartbeat函数
	r.lead = r.id						//将leader字段当前节点的ID
	r.state = StateLeader				//更新当前节点的角色
	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)	//获取当前节点中所有未提交的Entry记录
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}
	//检测未提交的记录中是否存在多条集群配置变更的Entry记录(即EntryConfChange类型的Entry记录)
	nconf := numOfPendingConf(ents)
	if nconf > 1 {						//若存在多条EntryConfChange类型记录，则异常关闭
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {						//如果只有一条，则将pengdingConf设置为true
		r.pendingConf = true
	}

	r.appendEntry(pb.Entry{Data: nil})	//向当前节点的raftLog中追加一条空的Entry记录
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}
//该方法除了完成状态切换，还会向集群中的其他节点发送相应类型的消息。例如,如果当前Follower节点要切换成PreCandidate状态，则会发送MsgPreVote消息。
func (r *raft) campaign(t CampaignType) {
	var term uint64						//在该方法的最后，会发送一条消息，这里term和voteMsg分别用于确定该消息的Term值和类型
	var voteMsg pb.MessageType
	if t == campaignPreElection {		//切换的目标状态是PreCandidate
		r.becomePreCandidate()			//将当前节点切换成PreCandidate状态
		voteMsg = pb.MsgPreVote			//确定最后发送的消息是MsgPreVote类型
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1				//确定最后发送消息的Term值，这里只是增加了消息的Term值，并未增加raft.Term字段的值
	} else {							//切换的目标状态是Candidate
		r.becomeCandidate()				//将当前节点切换成Candidate状态
		voteMsg = pb.MsgVote			//确定最后发送的消息是MsgPreVote类型
		term = r.Term					//确定最后发送消息的Term值
	}
	//统计当前节点收到的选票，并统计其得票数是否超过半数，这次检测主要是为单节点设置的
	if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		// We won the election after voting for ourselves (which must mean that  当得到足够的选票时，则将PreCandidate状态的节点切换成Candidate状态
		// this is a single-node cluster). Advance to the next state.            ，Candidate状态的节点则切换成Leader状态
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	for id := range r.prs {			//状态切换完成之后，当前节点会向集群中所有节点发送指定类型的消息
		if id == r.id {					//跳过当前节点自身
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)
		//在进行Leader节点转移时，MsgPreVote或MsgVote消息会在Context字段中设置该特殊值
		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		//发送指定类型的消息，其中Index和LogTerm分别是当前节点的raftLog中最后一条消息的Index值和Term值
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}
//该方法会将收到的投票结果记录到raft.votes字段中，之后通过统计该字段从而确定该节点的得票数
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {							//根据MsgPreVoteResp消息的Reject字段输出相应的日志
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {	//记录集群中其他节点的投票结果
		r.votes[id] = v
	}
	for _, vv := range r.votes {	//统计投票结果并返回
		if vv {
			granted++
		}
	}
	return granted
}
//集群启动一段时间之后，会有一个Follower节点的选举计时器超时，此时就会创建MsgHUP消息(其中Term为0)并调用raft.Setp()方法。该方法是etcd-raft模块
//处理各类消息的入口，它会根据节点的状态及处理的消息类型将其分成不同的代码片段进行介绍。该方法主要分为两部分：第一部分是根据Term值对消息进行
//分类处理，第二部分是根据消息的类型进行分类处理。
func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {					//首先根据消息的Term值进行分类处理
	case m.Term == 0:			//对本地消息并没有做什么处理。这里的MsgHUP消息Term值为0，就是本地消息的一种；后面的MsgProp和MsgReadIndex也是本地消息
		// local message
	case m.Term > r.Term:		//在上述场景中，当收到MsgPreVote消息(Term字段为1)时，集群中的其他Follower节点的Term值都为0
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {			//这里只对MsgVote和MsgPreVote两种类型消息进行处理
			//根据消息的Context字段判断收到的MsgPreVote消息是否为Leader节点转移场景下产生的，如果是，则强制当期节点参与本地预选(或选举)
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			//通过一系列条件判断当前节点是否参与此次选举，其中主要检测集群是否开启了CheckQuorum模式、当前节点是否有已知的Leader节点，以及其选举计时器的时间
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil		//当前节点不参与此次选举
			}
		}
		switch {				//在这个switch中，当前节点会根据消息类型决定是否切换状态
		case m.Type == pb.MsgPreVote:	//收到MsgPreVote消息时，不会引起当前节点的状态切换
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)			//将当前节点切换成Follower状态
			}
		}

	case m.Term < r.Term:
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {						//根据Message的Type进行分类处理
	case pb.MsgHup:							//针对MsgHUP类型的消息进行处理
		if r.state != StateLeader {			//只有非Leader状态的节点才会处理MsgHUP消息
			//获取raftLog中已提交但未应用(即applied~committed)的Entry记录
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			//检测是否有未应用的EntryConfChange记录，如果有就放弃发起选举的机会
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			if r.preVote {			//检测当前集群是否开启了PreVote模式，如果开启了，则先切换到调用raft.compaign方法切换当前节点的角色,发起PreVote
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {					//如果当前节点已经是Leader状态，则仅仅输出一条Debug日志
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:	//对MsgVote和MsgPreVote消息的处理
		//当前节点在参与预选时，会综合下面几个条件决定是否投票：1、当前节点是否已经投过票；2、MsgPreVote消息发送者的任期号是否更大；
		//3、当期节点投票给了对方节点；4、MsgPreVote消息发送者的raftLog中是否包含当前节点的全部Entry记录
		if r.isLearner {
			// TODO: learner may need to vote, in case of node down when confchange.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			return nil
		}
		// The m.Term > r.Term clause is for MsgPreVote. For MsgVote m.Term should
		// always equal r.Term.
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why consider the
			// case where a single node was previously partitioned away and
			// it's local term is now of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})	//将票投给MsgPreVote消息的发送节点
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {					//不满足上述投赞同票条件时，当前节点会返回拒绝票(即响应消息中的Reject字段会设置成true)
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:						//对于其他类型消息的处理
		r.step(r, m)				//当前节点是Follower状态，raft.step字段指向stepFollower()函数
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message)

func stepLeader(r *raft, m pb.Message) {
	// These message types do not require any progress for m.From.
	//下面主要注册MsgBeat、MsgCheckQuorum、MsgProp、MsgReadIndex消息，这些消息无需处理消息的From字段和消息发送者对应的Progress实例可以直接
	//进行处理。
	switch m.Type {
	case pb.MsgBeat:					//MsgBeat消息的处理
		r.bcastHeartbeat()				//向所有节点发送心跳
		return
	case pb.MsgCheckQuorum:				//MsgCheckQuorum消息的处理
		if !r.checkQuorumActive() {		//用于检测当前Leader节点是否与集群中大部分节点连通，如果不连通，则切换成Follower状态
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		return
	case pb.MsgProp:
		if len(m.Entries) == 0 {		//检测MsgProp消息是否携带了Entry记录，如果未携带，则输出异常日志并终止程序
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if _, ok := r.prs[r.id]; !ok {	//检测当前节点是否被移除集群，如果当前节点以Leader状态被移除集群，则不再处理MsgProp消息
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		if r.leadTransferee != None {	//检测当前是否正在进行Leader节点的转移，不再处理MsgProp消息
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}

		for i, e := range m.Entries {	//遍历MsgProp消息携带的全部Entry记录
			//如果存在EntryConfChange类型的Entry记录，则将raft.pendingConf设置为true
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {		//如果存在多条EntryConfChange类型的记录，则只保留一条
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e.String())
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				}
				r.pendingConf = true
			}
		}
		r.appendEntry(m.Entries...)		//将上述Entry记录追加到当前节点的raftLog中
		r.bcastAppend()
		return
	case pb.MsgReadIndex:
		if r.quorum() > 1 {				//集群场景
			//Leader节点检测自身在当前任期中是否已提交过Entry记录，如果没有，则无法进行读取操作，直接返回
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:					//记录当前节点的raftLog.committed字段值，即已提交的位置
				r.readOnly.addRequest(r.raftLog.committed, m)	//将已提交位置(raftLog.committed)及MsgReadIndex消息的相关信息记录到raft.readOnly中
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)		//向集群其他节点发送MsgHeartbeat消息(发送心跳)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed		//检测是否开启了CheckQuorum模式，并据此更新返回给Follower的已提交位置
				//ReadOnlyLeaseBased模式下，Leader节点直接并不会发送任何消息来确认自身身份
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {								//单节点的情况
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return
	}

	// All other message types require a progress for m.From (pr).	//根据消息的From字段获取对应的Progress实例
	pr := r.getProgress(m.From)
	if pr == nil {								//没有操作到对应的Progress实例(该节点可能已从集群中删除)时，直接返回
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}
	switch m.Type {
	case pb.MsgAppResp:
		//更新对应Progress实例的RecentActive字段，从Leader节点的角度来看，MsgAppResp消息的发送节点还是存活的
		pr.RecentActive = true

		if m.Reject {							//MsgAPP消息被拒绝
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			//通过MsgAppResp消息携带的信息及对应的Progress状态，重新设置其Next值
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				//若对应的Progress处于ProgressStateReplicate状态，则切换成ProcessStateProbe状态，试探Follower的匹配位置(这里的试探是指发送一条
				//消息并等待其相应之后，再发送后续的消息)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				//再次向对应Follower节点发送MsgAPP消息
				r.sendAppend(m.From)
			}
		} else {								//之前发送的MsgApp消息已经被对应的Follower节点接收(Entry记录被成功追加)
			oldPaused := pr.IsPaused()
			//MsgAppResp消息的Index字段是对应Follower节点raftLog中最后一条Entry记录的索引，这里会根据该值更新其对应Progress实例的Match和Next
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					//一旦MsgAPP被Follower节点接收，则表示已经找到其正确的Next和Match，不必再进行“试探”这里将对应的Progress.state切换为ProgressStateReplicate
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					//之前由于某些原因，Leader节点通过发送快照的方式恢复Follower节点，但在发送MsgSnap消息的过程中，Follower节点恢复，并 正常接收了
					//Leader节点的MsgAPP消息，此时会丢弃MsgSnap消息，并开始“试探”该Follower节点正确的Match和Next值
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					//之前向某个Follower节点发送MsgAPP消息时，会将其相关信息保存到对应的Progress.ins中，在这里收到相应的MsgAppResp响应之后，会将其从ins中删除，
					//这样可以实现限流的效果，避免网络出现延迟时，继续发送消息，从而导致网络更加拥堵
					pr.ins.freeTo(m.Index)
				}
				//收到一个Follower节点的MsgAppResp消息之后，除了修改相应的Match和Next值，还会尝试更新raftLog.committed，因为有些Entry记录可能在此复制
				//中被保存到了半数以上的节点中
				if r.maybeCommit() {
					//向所有节点发送MsgAPP消息，注意，此次MsgAPP消息的Commit字段与上次MsgAPP消息已经不同
					r.bcastAppend()
				} else if oldPaused {				//之前是pause状态，现在可以任性的发消息了
					// update() reset the wait state on this node. If we had delayed sending
					// an update before, send it now.
					//之前Leader节点暂停向该Follower节点发送消息，收到MsgAppResp消息后，在上述代码中已经重置了相应状态，所以可以继续发送MsgAPP消息
					r.sendAppend(m.From)
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:					//MsgHeartbeatResp消息的处理
		pr.RecentActive = true					//更新对应Progress.RecentActive，表示Follower节点与自己连通
		pr.resume()								//重置Progress.Paused字段，表示可以继续向Follower节点发送消息

		// free one slot for the full inflights window to allow progress.
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()				//释放inflights中第一个消息，这样就可以开始后续消息的发送
		}
		//当Leader节点收到Follower节点的MsgHeartbeat消息之后，会比较对应的Match值与Leader节点的raftLog，从而判断Follower节点是否拥有了全部的Entry记录
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)				//通过向指定节点发送MsgAPP消息完成Entry记录的复制
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}

		ackCount := r.readOnly.recvAck(m)		//统计目前为止响应上述携带消息ID的MsgHeartbeat消息的节点个数
		if ackCount < r.quorum() {
			return
		}
		//响应节点超过半数之后，会清空readOnly中指定消息ID及其之前的所有相关记录
		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				//根据MsgReadIndex消息的From字段，判断该MsgReadIndex消息是否为Follower节点转发到Leader节点的消息。如果是客户端直接发送到Leader节点
				//的消息，则将MsgReadIndex消息对应的已提交位置以及其消息ID封装成ReadState实例，添加到raft.readStates中保存。后续会有其他goroutine读取
				//该数组，并对相应的MsgReadIndex消息进行响应
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				//如果是其他Follower节点转发到Leader节点的MsgReadIndex消息，则Leader节点会向Follower节点返回相应的MsgReadIndexResp消息，并由Follower
				//节点响应Client
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != ProgressStateSnapshot {			//检测Follower节点对应的状态是否为ProgressStateSnapshot
			return
		}
		if !m.Reject {									//之前的发送MsgSnap消息时出现异常
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {										//发送MsgSnap消息失败，这里会清空对Progress.PendingSnapshot字段
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the msgAppResp from the remote node before sending 无论MSPSnap消息发送是否失败，都会将对应Progress切换成ProgressStateProbe状态，
		// out the next msgApp.                                                            之后单条发送消息进行试探
		// If snapshot failure, wait for a heartbeat interval before next try
		// 暂停Leader节点向该Follower节点继续发送消息，如果发送MsgSnap消息成功，则待Leader节点收到相应的响应消息(MsgAppResp消息)，即可继续发送后续MsgApp消息，Leader节点
		// 对MsgAppResp消息的处理已经介绍过；如果发送MsgSnap消息失败，则Leader节点会等到收到MsgHeartbeatResp消息时，才会重新开始发送后续消息。
		pr.pause()
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,  	当Follower节点变得不可达，如果继续发送MsgApp消息，则会有大量消息丢失
		// there is huge probability that a MsgApp is lost.
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()													//将Follower节点对应的Progress实例切换成ProgressStateProbe状态
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {			//根据当前节点的状态决定其能够处理的选举响应信息的类型
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MsgApp:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:		//处理收到的选举响应信息
		gr := r.poll(m.From, m.Type, !m.Reject)			//记录并统计投票结果
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
		switch r.quorum() {							//得票是否过半数
		case gr:
			if r.state == StatePreCandidate {			//当PreCandidate节点在预选中收到半数以上的选票之后，会发起正式的选举
				r.campaign(campaignElection)
			} else {
				//当前节点切换成Leader状态，其中会重置每个节点对应的Next和Match两个索引。
				r.becomeLeader()						//赞同票与拒绝票相等时，无法获取半数以上的票数，当前节点切换成Follower状态，等待下一轮的选举
				r.bcastAppend()							//向集群中其他节点广播MsgAPP消息
			}
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)				//选举失败时，将当前节点切换成Follower状态，等待下一轮选举
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
}

func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None {			//当前集群中没有Leader节点，则忽略该MsgProp消息
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return
		}
		m.To = r.lead				//将消息的To字段设置为当前Leader节点的id
		r.send(m)					//将MsgProp消息发送到当前的Leader节点
	case pb.MsgApp:
		r.electionElapsed = 0		//重置选举计时器，防止当前Follower发起新一轮选举
		r.lead = m.From				//设置raft.Lead记录，保存当前集群的Leader节点ID
		r.handleAppendEntries(m)	//将MsgAPP消息中携带的Entry记录追加到raftLog中，并且向Leader节点发送MsgAppResp消息，响应此次MsgAPP消息
	case pb.MsgHeartbeat:			//MsgHeartbeat消息的处理
		r.electionElapsed = 0		//重置选举计时器，防止当前Follower节点发起新一轮选举
		r.lead = m.From				//设置Leader节点的id
		//其中会修改raft.committed字段的值，然后发送MsgHeartbeatResp类型消息，响应此次心跳
		r.handleHeartbeat(m)
	case pb.MsgSnap:				//MsgSnap消息的处理
		r.electionElapsed = 0		//重置raft.electionElapsed,防止发生选举
		r.lead = m.From				//设置Leader的id
		r.handleSnapshot(m)			//通过MsgSnap消息中的快照数据，重建当前节点的raftLog
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		if r.promotable() {
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		//在集群中，Follower节点不能直接响应客户端的只读请求，而是转发给Leader节点进行处理。在集群中，Follower节点不能直接响应客户端的只读请求，
		//而是转发给Leader节点进行处理，等待Leader节点响应之后，Follower节点才能响应Client。
		if r.lead == None {					//检测当前集群中的Leader节点
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)							//将MsgReadIndex消息转发给当前的Leader节点
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {			//检测MsgReadIndexResp消息的合法性
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		//将MsgReadIndex消息对应的消息ID以及已提交位置(raftLog.committed)封装成ReadState实例，并添加到raft.readStates中，等待其他goroutine来处理
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
}
//该方法首先会检测MsgAPP消息中携带的Entry记录是否合法，然后将这些Entry记录追加到raftLog中，最后创建相应的MsgAppResp消息。
func (r *raft) handleAppendEntries(m pb.Message) {
	//m.Index表示leader发送给follower的上一条日志的索引位置，如果Follower节点在Index位置的Entry记录已经提交过了，则不能进行追加操作，在前面
	//的Raft协议提过，已提交的记录不能被覆盖，所以Follower节点会将其committed位置通过MsgAppResp消息(Index字段)通知Leader节点
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}
	//尝试将消息携带的Entry记录追加到raftLog中
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		//若追加成功，则将最后一条记录的索引值通过MsgAppResp消息返回给Leader节点，这样Leader节点就可以根据此值更新其对应的Next和Match值
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		//若追加失败，则将失败消息返回给Leader节点(即MsgAppResp消息的Reject字段为true)，同时返回的还有一些提示信息(RejectHint字段保存了当前
		//节点raftLog中最后一条记录的索引)
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	//根据MsgHeartbeat消息的Commit字段，更新raftLog中记录的已提交位置，注意，在Leader节点发送MsgHeartbeat消息时，已经确定了当前Follower节点中
	//raftLog.committed字段的合适位置
	r.raftLog.commitTo(m.Commit)
	//发送MsgHeartbeatResp消息，响应心跳
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}
//该方法会读取MsgSnap消息中的快照数据，并重建当前节点的raftLog
func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term				//获取快照数据的元数据
	if r.restore(m.Snapshot) {															//返回值表示是否通过快照数据进行了重建
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		//向Leader节点返回MsgAppResp消息(Reject始终为false)。该MsgAppResp消息作为MsgSnap消息的响应与MsgApp消息并无差别
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	//根据快照数据的元数据查找匹配的Entry记录，如果存在，则表示当前节点已经拥有了该快照中的全部数据，所以无须进行后续重建操作
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		//快照中全部的Entry记录都已经提交了，所以尝试修改当前节点的raftLog.committed，而raftLog.committed只增不减
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	// The normal peer can't become learner.
	if !r.isLearner {
		for _, id := range s.Metadata.ConfState.Learners {
			if id == r.id {
				r.logger.Errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", r.id, s.Metadata.Index, s.Metadata.Term)
				return false
			}
		}
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	r.raftLog.restore(s)						//通过raftLog.unstable记录该快照数据，同事重置相关字段
	r.prs = make(map[uint64]*Progress)			//清空raft.prs，并根据快照的元数据进行重建
	r.learnerPrs = make(map[uint64]*Progress)
	r.restoreNode(s.Metadata.ConfState.Nodes, false)
	r.restoreNode(s.Metadata.ConfState.Learners, true)
	return true
}

func (r *raft) restoreNode(nodes []uint64, isLearner bool) {
	for _, n := range nodes {
		match, next := uint64(0), r.raftLog.lastIndex()+1
		if n == r.id {								//更新当前节点自身的Match值
			match = next - 1
			r.isLearner = isLearner
		}
		r.setProgress(n, match, next, isLearner)	//创建Progress实例，注意Match和Next的初始值
		r.logger.Infof("%x restored progress of %x [%s]", r.id, n, r.getProgress(n))
	}
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	_, ok := r.prs[r.id]
	return ok
}

func (r *raft) addNode(id uint64) {
	r.addNodeOrLearnerNode(id, false)
}

func (r *raft) addLearner(id uint64) {
	r.addNodeOrLearnerNode(id, true)
}

func (r *raft) addNodeOrLearnerNode(id uint64, isLearner bool) {
	r.pendingConf = false
	pr := r.getProgress(id)
	if pr == nil {
		r.setProgress(id, 0, r.raftLog.lastIndex()+1, isLearner)
	} else {
		if isLearner && !pr.IsLearner {
			// can only change Learner to Voter
			r.logger.Infof("%x ignored addLeaner: do not support changing %x from raft peer to learner.", r.id, id)
			return
		}

		if isLearner == pr.IsLearner {
			// Ignore any redundant addNode calls (which can happen because the
			// initial bootstrapping entries are applied twice).
			return
		}

		// change Learner to Voter, use origin Learner progress
		delete(r.learnerPrs, id)
		pr.IsLearner = false
		r.prs[id] = pr
	}

	if r.id == id {
		r.isLearner = isLearner
	}

	// When a node is first added, we should mark it as recently active.
	// Otherwise, CheckQuorum may cause us to step down if it is invoked
	// before the added node has a chance to communicate with us.
	pr = r.getProgress(id)
	pr.RecentActive = true
}

func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	r.pendingConf = false

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.prs) == 0 && len(r.learnerPrs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.state == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}

func (r *raft) resetPendingConf() { r.pendingConf = false }

func (r *raft) setProgress(id, match, next uint64, isLearner bool) {
	if !isLearner {
		delete(r.learnerPrs, id)
		r.prs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
		return
	}

	if _, ok := r.prs[id]; ok {
		panic(fmt.Sprintf("%x unexpected changing from voter to learner for %x", r.id, id))
	}
	r.learnerPrs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight), IsLearner: true}
}

func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
	delete(r.learnerPrs, id)
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from      该方法会检测当前集群中与当期Leader节点连通的节点个数是否超过了半数
// the view of the local raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
func (r *raft) checkQuorumActive() bool {
	var act int

	r.forEachProgress(func(id uint64, pr *Progress) {		//遍历集群中全部节点对应的Progress实例
		if id == r.id { // self is always active			当前节点自身对应的Progress实例
			act++
			return
		}
		//检测集群中其他节点的Progress.RecentActive。当Leader节点收到Follower节点返回的MsgAppResp消息时，会将其对应的Progress.RecentActive设置为
		//true。
		if pr.RecentActive && !pr.IsLearner {
			act++
		}

		pr.RecentActive = false								//重置RecentActive字段
	})

	return act >= r.quorum()								//检测与当前节点连通的节点个数是否超过半数
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
