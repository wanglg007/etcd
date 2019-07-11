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

type Watcher interface {
	EventChan() chan *Event
	StartIndex() uint64 // The EtcdIndex at which the Watcher was created
	Remove()
}
//watcher是客户端用来监听etcd服务端数据变化的一种手段。在客户端对指定路径节点添加watcher之后，如果该节点数据发生变化，则etcd服务端会通知相应的客户端。
type watcher struct {
	//当该watcher实例被修改操作触发时，会将对应的Event实例写入该通道中，后续由网络层读取该通道，并通知客户端此次修改。
	eventChan  chan *Event
	//标识当前watcher是否为stream类型的，当前stream watcher被触发一次后，并不会被自动删除，而是继续保持监听，并返回一系列监听到的Event
	stream     bool
	//标识当前watcher实例是否监听目标节点的子节点
	recursive  bool
	//标识该watcher实例从哪个CurrentIndex值开始监听对应的节点
	sinceIndex uint64
	//记录创建该watcher实例时对应的CurrentIndex值
	startIndex uint64
	//在watcherHub中维护了该wathcer实例与其监听的节点路径映射关系
	hub        *watcherHub
	//标记当前watcher实例是否已经被删除
	removed    bool
	//用于删除当前watcher实例的回调函数
	remove     func()
}

func (w *watcher) EventChan() chan *Event {
	return w.eventChan
}

func (w *watcher) StartIndex() uint64 {
	return w.startIndex
}
// 该方法负责将触发该watcher的Event实例写入eventChan通道中，在如下三种场景下当前watcher会被触发：(1)当修改操作发生在当前watcher实例监听的节点上时，会触发该watcher实例；
// (2)当前watcher实例不仅监听当前节点的变化，同时也监听其子节点的变化，当修改操作发生在子节点上时，也会触发该watcher实例；(3)当删除某个目录节点时，需要通知在其子节点
// 上监听的全部watcher实例。
// notify function notifies the watcher. If the watcher interests in the given path,
// the function will return true.
func (w *watcher) notify(e *Event, originalPath bool, deleted bool) bool {
	// watcher is interested the path in three cases and under one condition
	// the condition is that the event happens after the watcher's sinceIndex

	// 1. the path at which the event happens is the path the watcher is watching at.
	// For example if the watcher is watching at "/foo" and the event happens at "/foo",
	// the watcher must be interested in that event.

	// 2. the watcher is a recursive watcher, it interests in the event happens after
	// its watching path. For example if watcher A watches at "/foo" and it is a recursive
	// one, it will interest in the event happens at "/foo/bar".

	// 3. when we delete a directory, we need to force notify all the watchers who watches
	// at the file we need to delete.
	// For example a watcher is watching at "/foo/bar". And we deletes "/foo". The watcher
	// should get notified even if "/foo" is not the path it is watching.
	if (w.recursive || originalPath || deleted) && e.Index() >= w.sinceIndex {		//检查当前watcher是否监听对应节点的子节点 || 检查发生变化的是否正好是当前watcher监听节点 ||
		// We cannot block here if the eventChan capacity is full, otherwise        || 检查此次修改操作是否为删除操作 && 检查操作发生的时机是否为该watcher监听的范围
		// etcd will hang. eventChan capacity is full when the rate of
		// notifications are higher than our send rate.
		// If this happens, we close the channel.
		select {
		case w.eventChan <- e:											//将修改操作对应的Event写入eventChan通道等待处理
		default:
			// We have missed a notification. Remove the watcher.
			// Removing the watcher also closes the eventChan.
			w.remove()													//如果当前watcher.eventChan通道被填充满了，则会将该通道关闭，这可能导致事件丢失
		}
		return true
	}
	return false
}

// Remove removes the watcher from watcherHub
// The actual remove function is guaranteed to only be executed once
func (w *watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.eventChan)
	if w.remove != nil {
		w.remove()
	}
}

// nopWatcher is a watcher that receives nothing, always blocking.
type nopWatcher struct{}

func NewNopWatcher() Watcher                 { return &nopWatcher{} }
func (w *nopWatcher) EventChan() chan *Event { return nil }
func (w *nopWatcher) StartIndex() uint64     { return 0 }
func (w *nopWatcher) Remove()                {}
