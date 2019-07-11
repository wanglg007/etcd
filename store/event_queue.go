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
//该结构体是一个先进先出的、环形的Event队列
type eventQueue struct {
	Events   []*Event			//底层真正存储Event实例的数组
	Size     int				//当前Events字段中存储的Event实例个数
	Front    int				//eventQueue队列中第一个元素的下标值
	Back     int				//eventQueue队列中最后一个元素的下标值
	Capacity int				//Events字段的长度
}

func (eq *eventQueue) insert(e *Event) {
	eq.Events[eq.Back] = e							//添加Event实例
	eq.Back = (eq.Back + 1) % eq.Capacity			//后移Back

	if eq.Size == eq.Capacity { //dequeue			//当队列满了的时候，则后移Front，抛弃Front指向的Event实例
		eq.Front = (eq.Front + 1) % eq.Capacity
	} else {
		eq.Size++									//当队列没满的时候，增加Size
	}
}
