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

import "fmt"

const (
	// 表示Leader节点一次不能向目标节点发送多条消息，只能待一条消息被响应之后，才能发送下一条消息。当刚刚复制完快照数据、上次MsgAPP消息被拒绝(或是
	// 发送失败)或是Leader节点初始化时，都会导致目标节点的Progress切换到该状态；
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate		//表示正常的Entry记录复制状态，Leader节点向目标节点发送完消息之后，无需等待响应，即可开始后续消息的发送
	ProgressStateSnapshot		//表示Leader节点正在向目标节点发送快照数据
)

type ProgressStateType uint64

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string { return prstmap[uint64(st)] }

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64		//Match:对应Follower节点当前已经成功复制的Entry记录的索引值;Mext:对应Follower节点下一个待复制的Entry记录的索引值
	// State defines how the leader should interact with the follower.
	//
	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	State ProgressStateType	//对应Follower节点的复制状态

	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	Paused bool				//当前Leader节点是否可以向该Progress实例对应的Follower节点发送消息
	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	PendingSnapshot uint64	//当前正在发送的快照数据信息

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	RecentActive bool		//从当前Leader节点的角度来看，该Progress实例对应的Follower节点是否存活

	// inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo with the index of the last
	// received entry.
	ins *inflights

	// IsLearner is true if this progress is tracked for a learner.
	IsLearner bool
}

func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

func (pr *Progress) becomeProbe() {
	// If the original state is ProgressStateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}
// 该方法会尝试修改Match字段和Next字段，用来标识对应节点Entry记录复制的情况。Leader节点除了在向自身raftLog追加记录时(即appendEntry方法)会调用该方法
// 当Leader节点收到Follower节点的MsgAPPResponse消息(即MsgAPP消息的响应消息)时，也会调用该方法尝试修改Follower节点对应的Progress实例。
// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n		//n之前的成功发送所有的Entry记录已经写入对应节点的raftLog中
		updated = true
		pr.resume()			//将Progress.paused设置为false，表示Leader节点可以继续向对应Follower节点发送MsgAPP消息(即复制Entry记录)
	}
	if pr.Next < n+1 {
		pr.Next = n + 1		//移动Next字段，下次要复制的Entry记录从Next开始
	}
	return updated
}

func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }
// 该方法会根据对应Progress状态和MsgAppResp消息携带的提示信息，完成Progress.Next的更新。
// maybeDecrTo returns false if the given to index comes from an out of order message.     该方法的两个参数都是MsgAppResp消息携带信息：reject是被拒绝MsgAPP消息的Index值，last是被拒
// Otherwise it decreases the progress next index to min(rejected, last) and returns true. 绝MsgAppResp消息的RejectHint字段值(即对应Follower节点raftLog中最后一条Entry记录的索引)
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {			//出现过时的MsgAppResp消息
			return false
		}
		// directly decrease next to match + 1
		//根据前面对MsgAPP消息发送过程的分析，处于ProgressStateReplicate状态时，发送MsgAPP消息的同事会直接调用Progress.optimisticUpdate方法增加
		//Next、这就使得Next可能会比Match大很多，这里回退Next至Match位置，并在后面重试发送MsgAPP消息进行尝试
		pr.Next = pr.Match + 1
		return true
	}

	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next-1 != rejected {				//出现过时的MsgAppResp消息，直接忽略
		return false
	}
	//根据MsgAppResp携带的信息重置Next
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1							//将Next重置为1
	}
	pr.resume()								//Next重置完成，恢复消息发送，并在后面重新发送MsgAPP消息
	return true
}

func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

// IsPaused returns whether sending log entries to this node has been  该方法会检测当前节点是否能够向对应的节点发送消息
// paused. A node may be paused because it has rejected recent
// MsgApps, is currently waiting for a snapshot, or has reached the
// MaxInflightMsgs limit.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:		//ProgressStateProbe状态时检测Paused字段
		return pr.Paused
	case ProgressStateReplicate:	//ProgressStateReplicate状态时检测已发送未响应的消息个数
		return pr.ins.full()
	case ProgressStateSnapshot:		//ProgressStateSnapshot状态时始终可以发送消息
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

// needSnapshotAbort returns true if snapshot progress's Match
// is equal or higher than the pendingSnapshot.
func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}
//该结构体的主要功能是记录当前节点已发出但未收到响应的MsgAPP消息
type inflights struct {
	// the starting index in the buffer				inflights.buffer数组被当做一个环形数组使用，start字段中记录buffer中第一条MsgAPP消息的下标
	start int
	// number of inflights in the buffer			当前inflights实例中记录的MsgApp消息个数
	count int

	// the size of the buffer						当前inflights实例中能够记录的MsgAPP消息个数的上限
	size int

	// buffer contains the index of the last entry   用来记录MsgAPP消息相关信息的数组，其中记录的是MsgAPP消息中最后一条Entry记录的索引值
	// inside one message.
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add adds an inflight into inflights		该方法用来记录发送出去的MsgAPP消息
func (in *inflights) add(inflight uint64) {
	if in.full() {							//检测当前buffer数组是否已经被填充满了
		panic("cannot add into a full inflights")
	}
	next := in.start + in.count				//获取新增消息的下标
	size := in.size
	if next >= size {						//环形队列
		next -= size
	}
	if next >= len(in.buffer) {				//初始化时的buffer数组较短，随着使用会不断进行扩容(两倍)，但其扩容的上限为size
		in.growBuf()
	}
	in.buffer[next] = inflight				//在next位置记录消息中最后一条Entry记录的索引值
	in.count++								//递增count字段
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *inflights) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}
// 当Leader节点收到MsgAppResp消息时，会通过该方法将指定消息及其之前的消息全部清空，释放inflights空间，让后面的消息继续发送。
// freeTo frees the inflights smaller or equal to the given `to` flight.
func (in *inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {		//边界检测，检测当前inflights是否为空，以及参数to是否有效
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {						//从start开始遍历buffer
		if to < in.buffer[idx] { // found the first large inflight  查找第一个大于指定索引值的位置
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {							//因为是环形队列，如果idx越界，则从0开始继续遍历
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i										//i记录了此次释放的消息个数
	in.start = idx										//从start~idx的所有消息都被释放(注意，是环形队列)
	if in.count == 0 {									//inflights中全部消息都被清空了，则重置start
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}
//该方法只是否其中记录的第一条MsgAPP消息
func (in *inflights) freeFirstOne() { in.freeTo(in.buffer[in.start]) }

// full returns true if the inflights is full.	该方法用来判断当前inflights实例是否被填满
func (in *inflights) full() bool {
	return in.count == in.size
}

// resets frees all inflights.
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
