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

import (
	"container/list"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	etcdErr "github.com/coreos/etcd/error"
)

// A watcherHub contains all subscribed watchers
// watchers is a map with watched path as key and watcher as value
// EventHistory keeps the old events for watcherHub. It is used to help
// watcher to get a continuous event history. Or a watcher might miss the
// event happens between the end of the first watch command and the start
// of the second command.
type watcherHub struct {
	// count must be the first element to keep 64-bit alignment for atomic
	// access

	count int64 // current number of watchers.			当前watcherHub实例中保存的watcher实例个数

	mutex        sync.Mutex
	//维护节点与监听该节点的watcher实例的对应关系，其中key是node.Path字段值，value则是监听该节点的watcher实例列表
	watchers     map[string]*list.List
	//保存最近发生的修改操作对应的Event实例。当EventHistory的容量达到上限之后，继续向其中添加Event实例，则会自动删除最早添加的Event实例
	EventHistory *EventHistory
}

// newWatchHub creates a watcherHub. The capacity determines how many events we will
// keep in the eventHistory.
// Typically, we only need to keep a small size of history[smaller than 20K].
// Ideally, it should smaller than 20K/s[max throughput] * 2 * 50ms[RTT] = 2000
func newWatchHub(capacity int) *watcherHub {
	return &watcherHub{
		watchers:     make(map[string]*list.List),
		EventHistory: newEventHistory(capacity),
	}
}
// 当客户端请求etcd服务端为某个指定的节点添加watcher时，是通过该方法完成的。在添加watcher之前，会先查找EventHistory确定sinceIndex~CurrentIndex之间是否发生了触发
// 待添加watcher的操作。
// Watch function returns a Watcher.
// If recursive is true, the first change after index under key will be sent to the event channel of the watcher.
// If recursive is false, the first change after index at key will be sent to the event channel of the watcher.
// If index is zero, watch will start from the current index + 1.
func (wh *watcherHub) watch(key string, recursive, stream bool, index, storeIndex uint64) (Watcher, *etcdErr.Error) {
	reportWatchRequest()
	event, err := wh.EventHistory.scan(key, recursive, index)		//在EventHistory中，从index开始查找对应的Event

	if err != nil {
		err.Index = storeIndex
		return nil, err
	}

	w := &watcher{
		eventChan:  make(chan *Event, 100), // use a buffered channel
		recursive:  recursive,				//根据recursive参数设置是否监听指定节点的子节点
		stream:     stream,
		sinceIndex: index,					//根据index参数设置从哪个事件开始监听
		startIndex: storeIndex,				//创建该watcher实例时对应的CurrentIndex值
		hub:        wh,						//关联的watcherHub实例
	}

	wh.mutex.Lock()							//相关的加锁和解锁操作
	defer wh.mutex.Unlock()
	// If the event exists in the known history, append the EtcdIndex and return immediately
	if event != nil {						//如果在EventHistory中存在合适的Event，则表示从sinceIndex到CurrentIndex为止，发生了触发该watcher的操作
		ne := event.Clone()
		ne.EtcdIndex = storeIndex
		w.eventChan <- ne					//将查找到的Event写入eventChan通道中等待处理
		return w, nil						//直接返回watcher
	}
	//如果未查找到合适的Event，则表示从sinceIndex到CurrentIndex为止，并没有发生过触发watcher的操作
	l, ok := wh.watchers[key]				//查找节点对应的watcher列表

	var elem *list.Element

	if ok { // add the new watcher to the back of the list		将当前watcher添加到列表尾部
		elem = l.PushBack(w)
	} else { // create a new list and add the new watcher		第一次为节点添加watcher时，会先创建对应的watcher列表
		l = list.New()
		elem = l.PushBack(w)
		wh.watchers[key] = l
	}

	w.remove = func() {									//初始化当前watcher.remove字段
		if w.removed { // avoid removing it twice		检测当前watcher实例是否已经被删除
			return
		}
		w.removed = true								//标记当前watcher已被删除
		l.Remove(elem)									//将当前watcher从watcherHub中删除
		atomic.AddInt64(&wh.count, -1)			//更新watcherHub中保存的watcher个数
		reportWatcherRemoved()
		if l.Len() == 0 {								//如果没有任何监听某节点，则将该节点对应的watcher列表删除
			delete(wh.watchers, key)
		}
	}

	atomic.AddInt64(&wh.count, 1)						//更新当前watcherHub中记录的watcher数量
	reportWatcherAdded()

	return w, nil
}

func (wh *watcherHub) add(e *Event) {
	wh.EventHistory.addEvent(e)
}

// notify function accepts an event and notify to the watchers.		该方法触发监听对应节点的watcher
func (wh *watcherHub) notify(e *Event) {
	//首先将修改操作对应的Event实例保存到EventHistory中
	e = wh.EventHistory.addEvent(e) // add event into the eventHistory
	//按照"/"切分发生修改的节点的路径
	segments := strings.Split(e.Node.Key, "/")

	currPath := "/"

	// walk through all the segments of the path and notify the watchers   遍历切分结果，并调用notifyWatchers()方法，通知当前节点和所有父节点上的watcher，这里
	// if the path is "/foo/bar", it will notify watchers with path "/",   举个例子，如果"/foor/bar"节点被修改了，那么不仅监听"/foor/bar"节点的watcher会被触发，
	// "/foo" and "/foo/bar"                                               监听"/foo"节点的watcher也可能被触发

	for _, segment := range segments {
		currPath = path.Join(currPath, segment)
		// notify the watchers who interests in the changes of current path
		wh.notifyWatchers(e, currPath, false)
	}
}
//该方法会查找节点对应的watcher列表，并逐一触发列表中的watcher实例，当watcher实例(stream watcher除外)被成功触发之后，则会将其清除。
func (wh *watcherHub) notifyWatchers(e *Event, nodePath string, deleted bool) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	l, ok := wh.watchers[nodePath]												//获取监听指定节点的watcher列表
	if ok {
		curr := l.Front()

		for curr != nil {														//遍历该watcher列表
			next := curr.Next() // save reference to the next one in the list

			w, _ := curr.Value.(*watcher)										//获取watcher实例

			originalPath := (e.Node.Key == nodePath)
			//调用watcher.notify()方法，触发相应的watcher
			if (originalPath || !isHidden(nodePath, e.Node.Key)) && w.notify(e, originalPath, deleted) {
				if !w.stream { // do not remove the stream watcher				检测是否为stream watcher，如果不是，则需要将该watcher清除
					// if we successfully notify a watcher
					// we need to remove the watcher from the list
					// and decrease the counter
					w.removed = true
					l.Remove(curr)												//从当前watcher列表中删除
					atomic.AddInt64(&wh.count, -1)						//更新count字段
					reportWatcherRemoved()
				}
			}

			curr = next // update current to the next element in the list		继续处理watcher列表中的下一个watcher实例
		}
		//如果列表中的watcher实例全部被清空，则将列表中watcherHub中清除
		if l.Len() == 0 {
			// if we have notified all watcher in the list
			// we can delete the list
			delete(wh.watchers, nodePath)
		}
	}
}

// clone function clones the watcherHub and return the cloned one.
// only clone the static content. do not clone the current watchers.
func (wh *watcherHub) clone() *watcherHub {
	clonedHistory := wh.EventHistory.clone()

	return &watcherHub{
		EventHistory: clonedHistory,
	}
}

// isHidden checks to see if key path is considered hidden to watch path i.e. the
// last element is hidden or it's within a hidden directory
func isHidden(watchPath, keyPath string) bool {
	// When deleting a directory, watchPath might be deeper than the actual keyPath
	// For example, when deleting /foo we also need to notify watchers on /foo/bar.
	if len(watchPath) > len(keyPath) {
		return false
	}
	// if watch path is just a "/", after path will start without "/"
	// add a "/" to deal with the special case when watchPath is "/"
	afterPath := path.Clean("/" + keyPath[len(watchPath):])
	return strings.Contains(afterPath, "/_")
}
