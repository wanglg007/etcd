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

package store

const (
	Get              = "get"
	Create           = "create"
	Set              = "set"
	Update           = "update"
	Delete           = "delete"
	CompareAndSwap   = "compareAndSwap"
	CompareAndDelete = "compareAndDelete"
	Expire           = "expire"
)
//当V2存储收到调用请求之后，会进行相应的处理，在处理完成之后，会将处理结果及相关信息封装成Event实例返回。
type Event struct {
	Action    string      `json:"action"`				//该Event实例对应的操作，可选项有Get、Create、Set、Update、Delete、CompareAndSwap、CompareAndDelete和Expire
	Node      *NodeExtern `json:"node,omitempty"`		//当前操作节点对应的NodeExtern实例
	PrevNode  *NodeExtern `json:"prevNode,omitempty"`	//如果是更新操作，则记录该节点之前状态对应的NodeExtern实例
	EtcdIndex uint64      `json:"-"`					//记录操作完成之后的CurrentIndex值
	Refresh   bool        `json:"refresh,omitempty"`	//如果是Set、Update、CompareAndSwap三种涉及值更新的操作，该该字段都有可能被设置true。当该字段被设置为true，表示
														//该Event实例对应的修改操作只进行刷新操作，并没有改变节点的值，不会触发相关的watcher。
}

func newEvent(action string, key string, modifiedIndex, createdIndex uint64) *Event {
	n := &NodeExtern{
		Key:           key,
		ModifiedIndex: modifiedIndex,
		CreatedIndex:  createdIndex,
	}

	return &Event{
		Action: action,
		Node:   n,
	}
}

func (e *Event) IsCreated() bool {
	if e.Action == Create {
		return true
	}
	return e.Action == Set && e.PrevNode == nil
}

func (e *Event) Index() uint64 {
	return e.Node.ModifiedIndex
}

func (e *Event) Clone() *Event {
	return &Event{
		Action:    e.Action,
		EtcdIndex: e.EtcdIndex,
		Node:      e.Node.Clone(),
		PrevNode:  e.PrevNode.Clone(),
	}
}

func (e *Event) SetRefresh() {
	e.Refresh = true
}
