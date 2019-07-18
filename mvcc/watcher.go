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

package mvcc

import (
	"bytes"
	"errors"
	"sync"

	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	ErrWatcherNotExist = errors.New("mvcc: watcher does not exist")
)

type WatchID int64

// FilterFunc returns true if the given event should be filtered out.
type FilterFunc func(e mvccpb.Event) bool

type WatchStream interface {
	// Watch creates a watcher. The watcher watches the events happening or
	// happened on the given key or range [key, end) from the given startRev.
	//
	// The whole event history can be watched unless compacted.
	// If `startRev` <=0, watch observes events after currentRev.
	//
	// The returned `id` is the ID of this watcher. It appears as WatchID
	// in events that are sent to the created watcher through stream channel.
	//
	Watch(key, end []byte, startRev int64, fcs ...FilterFunc) WatchID

	// Chan returns a chan. All watch response will be sent to the returned chan.
	Chan() <-chan WatchResponse

	// RequestProgress requests the progress of the watcher with given ID. The response
	// will only be sent if the watcher is currently synced.
	// The responses will be sent through the WatchRespone Chan attached
	// with this stream to ensure correct ordering.
	// The responses contains no events. The revision in the response is the progress
	// of the watchers since the watcher is currently synced.
	RequestProgress(id WatchID)

	// Cancel cancels a watcher by giving its ID. If watcher does not exist, an error will be
	// returned.
	Cancel(id WatchID) error

	// Close closes Chan and release all related resources.
	Close()

	// Rev returns the current revision of the KV the stream watches on.
	Rev() int64
}
//该结构体表示一次响应，其中封装了多个Event实例
type WatchResponse struct {
	// WatchID is the WatchID of the watcher this response sent to.				被触发的watcher实例的唯一标识
	WatchID WatchID

	// Events contains all the events that needs to send.						触发对应watcher事件集合
	Events []mvccpb.Event

	// Revision is the revision of the KV when the watchResponse is created.	当前watchResponse实例创建时对应的revision值
	// For a normal response, the revision should be the same as the last
	// modified revision inside Events. For a delayed response to a unsynced
	// watcher, the revision is greater than the last modified revision
	// inside Events.
	Revision int64

	// CompactRevision is set when the watcher is cancelled due to compaction.	如果因为压缩操作对应watcher实例被取消，则该字段设置为压缩操作对应的revision
	CompactRevision int64
}

// watchStream contains a collection of watchers that share
// one streaming chan to send out watched events and other control events.
type watchStream struct {
	watchable watchable						//用来记录关联的watchableStore实例
	ch        chan WatchResponse			//通过该watchStream实例创建的watcher实例在被触发时，都会将Event事件写入该通道中

	mu sync.Mutex // guards fields below it
	// nextID is the ID pre-allocated for next new watcher in this stream
	nextID   WatchID						//在当前watchStream实例中添加watcher实例时，会为其分配唯一标识，该字段用来生成该唯一标识
	closed   bool
	cancels  map[WatchID]cancelFunc			//该字段记录唯一标识与取消对应watcher的回调函数之间的映射关系
	watchers map[WatchID]*watcher			//该字段记录唯一标识与对应watcher实例之间的映射关系
}
// 该方法会创建watcher实例监听指定的Key(或是范围监听[key,end))，其中startRev参数则指定了该watcher实例监听的起始revision，如果startRev参数小于等于0，则表示
// 从当前revision开始监听，fcs参数是Event事件监听器。
// Watch creates a new watcher in the stream and returns its WatchID.
// TODO: return error if ws is closed?
func (ws *watchStream) Watch(key, end []byte, startRev int64, fcs ...FilterFunc) WatchID {
	// prevent wrong range where key >= end lexicographically
	// watch request with 'WithFromKey' has empty-byte range end
	if len(end) != 0 && bytes.Compare(key, end) != -1 {		//检测参数的合法性
		return -1
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {											//检测当前watchStream是否已经关闭
		return -1
	}

	id := ws.nextID											//生成新建watcher实例的唯一标识
	ws.nextID++
	//调用watchableStore.watcher()方法创建watcher实例，注意，新建的watcher实例共用了同一个通道
	w, c := ws.watchable.watch(key, end, startRev, id, ws.ch, fcs...)

	ws.cancels[id] = c										//记录新建的watcher实例
	ws.watchers[id] = w										//保存取消watcher实例时用到的回调函数
	return id
}

func (ws *watchStream) Chan() <-chan WatchResponse {
	return ws.ch
}
//该方法会查找并调用指定watcher实例对应的取消回调函数。
func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock()
	cancel, ok := ws.cancels[id]			//根据watcher唯一标识，查找对应的取消回调函数
	w := ws.watchers[id]
	ok = ok && !ws.closed
	ws.mu.Unlock()

	if !ok {
		return ErrWatcherNotExist
	}
	cancel()								//调用回调函数，取消watcher实例

	ws.mu.Lock()
	// The watch isn't removed until cancel so that if Close() is called,
	// it will wait for the cancel. Otherwise, Close() could close the
	// watch channel while the store is still posting events.
	if ww := ws.watchers[id]; ww == w {
		delete(ws.cancels, id)				//从watchStream.watchers字段中清理对应的watcher实例
		delete(ws.watchers, id)				//从watchStream.cancels中清理对应的取消回调函数
	}
	ws.mu.Unlock()

	return nil
}

func (ws *watchStream) Close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, cancel := range ws.cancels {
		cancel()
	}
	ws.closed = true
	close(ws.ch)
	watchStreamGauge.Dec()
}

func (ws *watchStream) Rev() int64 {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.watchable.rev()
}
//该方法用来检测指定watcher的处理速度(progress,即当前watcher正在处理哪个revision中的更新操作)。只有当watcher完全同步时，调用该方法才会创建一个空的WatchResponse，
//并写入watcher.cn通道中。
func (ws *watchStream) RequestProgress(id WatchID) {
	ws.mu.Lock()
	w, ok := ws.watchers[id]			//从watchStream.watchers中查询指定的watcher实例
	ws.mu.Unlock()
	if !ok {
		return
	}
	ws.watchable.progress(w)			//调用watchableStore.progress方法
}
