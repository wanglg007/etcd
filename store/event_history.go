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
	"fmt"
	"path"
	"strings"
	"sync"

	etcdErr "github.com/coreos/etcd/error"
)

type EventHistory struct {
	Queue      eventQueue				//用来存储Event实例的eventQueue实例
	StartIndex uint64					//当前EventHistory实例中记录的第一个Event实例的ModifiedIndex字段值
	LastIndex  uint64					//当前EventHistory实例中记录的最后一个Event实例的ModifiedIndex字段值，从而可知，EventHistory只记录了从StartIndex~LastIndex之间的Event
	rwl        sync.RWMutex				//在添加或读取Event实例时，都需要通过该锁进行同步
}

func newEventHistory(capacity int) *EventHistory {
	return &EventHistory{
		Queue: eventQueue{
			Capacity: capacity,
			Events:   make([]*Event, capacity),
		},
	}
}

// addEvent function adds event into the eventHistory		该方法添加新Event实例
func (eh *EventHistory) addEvent(e *Event) *Event {
	eh.rwl.Lock()											//通过rwl锁进行同步
	defer eh.rwl.Unlock()

	eh.Queue.insert(e)										//添加Event实例

	eh.LastIndex = e.Index()								//更新StartIndex字段和LastIndex字段

	eh.StartIndex = eh.Queue.Events[eh.Queue.Front].Index()

	return e
}
// 该方法会从index参数指定的位置开始查找EventHistory中是否记录了参数key指定节点对应的Event实例。
// scan enumerates events from the index history and stops at the first point
// where the key matches.
func (eh *EventHistory) scan(key string, recursive bool, index uint64) (*Event, *etcdErr.Error) {
	eh.rwl.RLock()										//加锁和解锁相关逻辑
	defer eh.rwl.RUnlock()

	// index should be after the event history's StartIndex
	if index < eh.StartIndex {							//检测index参数是否合法(即位于StartIndex~LastIndex之间)
		return nil,
			etcdErr.NewError(etcdErr.EcodeEventIndexCleared,
				fmt.Sprintf("the requested history has been cleared [%v/%v]",
					eh.StartIndex, index), 0)
	}

	// the index should come before the size of the queue minus the duplicate count
	if index > eh.LastIndex { // future index
		return nil, nil
	}

	offset := index - eh.StartIndex						//首先将index参数转换成eventQueue.Events的下标
	i := (eh.Queue.Front + int(offset)) % eh.Queue.Capacity

	for {												//遍历index之后的Event实例
		e := eh.Queue.Events[i]

		if !e.Refresh {									//过滤掉Refresh类型的修改操作
			ok := (e.Node.Key == key)					//根据Key匹配Event实例

			if recursive {								//key可能是个目录，根据recursive参数决定是否查找其子节点对应的Event
				// add tailing slash
				nkey := path.Clean(key)
				if nkey[len(nkey)-1] != '/' {
					nkey = nkey + "/"
				}

				ok = ok || strings.HasPrefix(e.Node.Key, nkey)
			}
			//针对Delete、Expire等操作额处理，比较的是Event.PrevNode
			if (e.Action == Delete || e.Action == Expire) && e.PrevNode != nil && e.PrevNode.Dir {
				ok = ok || strings.HasPrefix(key, e.PrevNode.Key)
			}

			if ok {
				return e, nil
			}
		}

		i = (i + 1) % eh.Queue.Capacity
		//整个EventHistory中不存在与参数key匹配的Event实例
		if i == eh.Queue.Back {
			return nil, nil
		}
	}
}

// clone will be protected by a stop-world lock
// do not need to obtain internal lock
func (eh *EventHistory) clone() *EventHistory {
	clonedQueue := eventQueue{
		Capacity: eh.Queue.Capacity,
		Events:   make([]*Event, eh.Queue.Capacity),
		Size:     eh.Queue.Size,
		Front:    eh.Queue.Front,
		Back:     eh.Queue.Back,
	}

	copy(clonedQueue.Events, eh.Queue.Events)
	return &EventHistory{
		StartIndex: eh.StartIndex,
		Queue:      clonedQueue,
		LastIndex:  eh.LastIndex,
	}

}
