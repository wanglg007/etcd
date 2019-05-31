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

package raft

import pb "github.com/coreos/etcd/raft/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	req   pb.Message								//记录了对应的MsgReadIndex请求
	index uint64									//该MsgReadIndex请求到达时，对应的已提交位置(raftLog.committed)
	acks  map[uint64]struct{}						//记录了该MsgReadIndex相关的MsgHeartbeatResp响应信息
}
//该字段的主要作用是批量处理只读请求
type readOnly struct {
	option           ReadOnlyOption					//当前只读请求的处理模式
	//在etcd服务端收到MsgReadIndex消息时，会为期创建一个消息ID，并作为MsgReadIndex消息的第一条Entry记录。在pendingReadIndex中记录了消息ID与对应请求的
	//的readIndexStatus实例的映射。
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string						//记录了MsgReadIndex请求对应的消息ID
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	ctx := string(m.Entries[0].Data)			//在ReadIndex消息的第一个记录中，记录了消息ID
	//检测是否存在ID相同的MsgReadIndex，如果存在，则不再记录该MsgReadIndex请求。创建MsgReadIndex对应的ReadIndexStatus实例，并记录到pendingReadIndex中
	if _, ok := ro.pendingReadIndex[ctx]; ok {
		return
	}
	ro.pendingReadIndex[ctx] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)	//记录消息ID
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(m pb.Message) int {
	rs, ok := ro.pendingReadIndex[string(m.Context)]	//获取消息ID对应的ReadIndexStatus实例
	if !ok {
		return 0
	}

	rs.acks[m.From] = struct{}{}						//表示MsgHeartbeatResp消息的发送节点与当前节点连通
	// add one to include an ack from local node
	return len(rs.acks) + 1							//统计目前为止与当前节点连通的节点个数(最后+1是当前节点自身)
}
// 该方法清空指定消息ID及其之前的所有相关记录
// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)							//MsgHeartbeat消息对应的MsgReadIndex消息ID
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {			//遍历readOnly中记录的消息ID
		i++
		rs, ok := ro.pendingReadIndex[okctx]			//查找消息ID对应的readIndexStatus实例
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)							//将ReadIndexStatus实例保存到rss数组中
		if okctx == ctx {								//查找指定的MsgReadIndex消息ID，则设置found变量并跳出循环
			found = true
			break
		}
	}
	//查找指定的MsgReadIndex消息ID，则清空readOnly中所有在它之前的消息ID及相关内容
	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]		//清理readOnly.readIndexQueue
		for _, rs := range rss {						//清理readOnly.pendingReadIndex
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
