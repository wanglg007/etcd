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

package mvcc

import (
	"fmt"
	"math"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/adt"
)

var (
	// watchBatchMaxRevs is the maximum distinct revisions that
	// may be sent to an unsynced watcher at a time. Declared as
	// var instead of const for testing purposes.
	watchBatchMaxRevs = 1000
)
//etcd的服务端一般会批量处理watcher事件，在结构体eventBatch中可以封装多个Event实例
type eventBatch struct {
	// evs is a batch of revision-ordered events
	evs []mvccpb.Event		//记录的Event实例时按照revision排序的
	// revs is the minimum unique revisions observed for this batch
	revs int				//记录当前eventBatch中记录的Event实例来自多少个不同的main revision，特别注意，是不相同的main revision值的个数
	// moreRev is first revision with more events following this batch
	// 由于当前eventBatch中记录的Event个数达到上限之后，后续Event实例无法加入该eventBatch中，该字段记录了无法加入该eventBatch实例的第一个Event实例对应的main revision值
	moreRev int64
}
//该方法将Event实例添加到eventBatch中
func (eb *eventBatch) add(ev mvccpb.Event) {
	if eb.revs > watchBatchMaxRevs {		//检测revs是否达到上限，如果达到上限，则直接返回
		// maxed out batch size
		return
	}

	if len(eb.evs) == 0 {					//第一次添加Event实例
		// base case
		eb.revs = 1
		eb.evs = append(eb.evs, ev)			//将Event实例添加到evs字段中保存
		return
	}

	// revision accounting
	ebRev := eb.evs[len(eb.evs)-1].Kv.ModRevision		//获取最后一个Event实例对应的main revision值
	evRev := ev.Kv.ModRevision							//新增Event实例对应的main revision值
	if evRev > ebRev {									//比较两个main revision值
		eb.revs++										//递增revs字段
		if eb.revs > watchBatchMaxRevs {
			//当前eventBatch实例中记录的Event实例已达到上限，则更新moreRev字段，记录最后一个无法加入的Event实例的main revision值
			eb.moreRev = evRev
			return
		}
	}
	//如果能继续添加Event实例，则将Event实例添加到evs中
	eb.evs = append(eb.evs, ev)
}

type watcherBatch map[*watcher]*eventBatch

func (wb watcherBatch) add(w *watcher, ev mvccpb.Event) {
	eb := wb[w]
	if eb == nil {
		eb = &eventBatch{}
		wb[w] = eb
	}
	eb.add(ev)
}

// newWatcherBatch maps watchers to their matched events. It enables quick
// events look up by watcher.
func newWatcherBatch(wg *watcherGroup, evs []mvccpb.Event) watcherBatch {
	if len(wg.watchers) == 0 {
		return nil
	}

	wb := make(watcherBatch)
	for _, ev := range evs {
		for w := range wg.watcherSetByKey(string(ev.Kv.Key)) {
			if ev.Kv.ModRevision >= w.minRev {
				// don't double notify
				wb.add(w, ev)
			}
		}
	}
	return wb
}

type watcherSet map[*watcher]struct{}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; ok {
		panic("add watcher twice!")
	}
	w[wa] = struct{}{}
}

func (w watcherSet) union(ws watcherSet) {
	for wa := range ws {
		w.add(wa)
	}
}

func (w watcherSet) delete(wa *watcher) {
	if _, ok := w[wa]; !ok {
		panic("removing missing watcher!")
	}
	delete(w, wa)
}

type watcherSetByKey map[string]watcherSet

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.key)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.key)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.key)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				// remove the set; nothing left
				delete(w, k)
			}
			return true
		}
	}
	return false
}

// watcherGroup is a collection of watchers organized by their ranges
type watcherGroup struct {
	// keyWatchers has the watchers that watch on a single key
	//该字段记录监听单个Key的watcher实例，watcherSetByKey中的key就是监听的原始Key值
	keyWatchers watcherSetByKey
	// ranges has the watchers that watch a range; it is sorted by interval
	// 该字段记录进行范围监听的watcher实例
	ranges adt.IntervalTree
	// watchers is the set of all watchers
	// 该字段记录当前watcherGroup实例中全部的watcher实例
	watchers watcherSet
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(watcherSetByKey),
		watchers:    make(watcherSet),
	}
}

// add puts a watcher in the group.
func (wg *watcherGroup) add(wa *watcher) {
	wg.watchers.add(wa)							//首先将watcher实例添加到watchers字段中保存
	if wa.end == nil {							//watcher.end为空表示只监听单个Key，则将其添加到keyWatchers中保存
		wg.keyWatchers.add(wa)
		return
	}
	// 如果待添加的是范围watcher，则执行下面的添加逻辑
	// interval already registered?
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))	//根据待添加watcher的key和end字段创建IntervalTree中的一个节点
	if iv := wg.ranges.Find(ivl); iv != nil {
		//如果在IntervalTree中查找到了对应节点，则将该watcher添加到对应的watcherSet中
		iv.Val.(watcherSet).add(wa)
		return
	}
	// 当前IntervalTree中没有对应节点，则创建对应的watcherSet，并添加到IntervalTree中
	// not registered, put in interval tree
	ws := make(watcherSet)
	ws.add(wa)
	wg.ranges.Insert(ivl, ws)
}

// contains is whether the given key has a watcher in the group.
func (wg *watcherGroup) contains(key string) bool {
	_, ok := wg.keyWatchers[key]
	return ok || wg.ranges.Intersects(adt.NewStringAffinePoint(key))
}

// size gives the number of unique watchers in the group.
func (wg *watcherGroup) size() int { return len(wg.watchers) }

// delete removes a watcher from the group.
func (wg *watcherGroup) delete(wa *watcher) bool {
	if _, ok := wg.watchers[wa]; !ok {
		return false
	}
	wg.watchers.delete(wa)
	if wa.end == nil {
		wg.keyWatchers.delete(wa)
		return true
	}

	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	iv := wg.ranges.Find(ivl)
	if iv == nil {
		return false
	}

	ws := iv.Val.(watcherSet)
	delete(ws, wa)
	if len(ws) == 0 {
		// remove interval missing watchers
		if ok := wg.ranges.Delete(ivl); !ok {
			panic("could not remove watcher from interval tree")
		}
	}

	return true
}
// 该方法会根据unsynced watcherGroup中记录的watcher个数对其进行分批返回。另外，它还会获取该批watcher实例中查找最小的minRev字段。
// choose selects watchers from the watcher group to update
func (wg *watcherGroup) choose(maxWatchers int, curRev, compactRev int64) (*watcherGroup, int64) {
	//当前unsynced watcherGroup中记录的watcher个数未达到指定上限(默认值为512)，则直接调用unsynced watcherGroup的chooseAll()方法获取所有未完成同步的watcher
	//中最小的minRev字段值
	if len(wg.watchers) < maxWatchers {
		return wg, wg.chooseAll(curRev, compactRev)
	}
	//如果当前unsynced watcherGroup中的watcher个数超过指定上限，则需要分批处理
	ret := newWatcherGroup()						//创建新的watcherGroup实例
	//从unsynced wtcherGroup中遍历得到指定量的watcher，并添加到新建的watcherGroup实例中
	for w := range wg.watchers {
		if maxWatchers <= 0 {
			break
		}
		maxWatchers--
		ret.add(w)
	}
	//依然通过chooseAll()方法从该批watcher实例中查找最小的minRev字段
	return &ret, ret.chooseAll(curRev, compactRev)
}
//该方法会遍历watcherGroup中记录的全部watcher实例，记录其中最小的minRev字段值并返回。
func (wg *watcherGroup) chooseAll(curRev, compactRev int64) int64 {
	minRev := int64(math.MaxInt64)						//用于记录该watcherGroup中最小的minRev字段
	for w := range wg.watchers {						//遍历watcherGroup中全部watcher实例
		if w.minRev > curRev {							//该watcher因为压缩操作而被删除
			// after network partition, possibly choosing future revision watcher from restore operation
			// with watch key "proxy-namespace__lostleader" and revision "math.MaxInt64 - 2"
			// do not panic when such watcher had been moved from "synced" watcher during restore operation
			if !w.restore {
				panic(fmt.Errorf("watcher minimum revision %d should not exceed current revision %d", w.minRev, curRev))
			}

			// mark 'restore' done, since it's chosen
			w.restore = false
		}
		if w.minRev < compactRev {
			select {
			//创建WatchResponse(注意，其CompactRevision字段是有值的)，并写入watcher.cn通道
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: compactRev}:
				w.compacted = true		//将watcher.compacted设置为false，表示因压缩操作而被删除
				wg.delete(w)			//将该watcher实例从watcherGroup中删除
			//如果watcher.cn通道阻塞，则下次调用chooseAll()方法时再尝试重新删除该watcher实例
			default:
				// retry next time
			}
			continue
		}
		if minRev > w.minRev {			//更新minRev遍历，记录最小的watcher.minRev字段
			minRev = w.minRev
		}
	}
	return minRev
}

// watcherSetByKey gets the set of watchers that receive events on the given key.
func (wg *watcherGroup) watcherSetByKey(key string) watcherSet {
	wkeys := wg.keyWatchers[key]
	wranges := wg.ranges.Stab(adt.NewStringAffinePoint(key))

	// zero-copy cases
	switch {
	case len(wranges) == 0:
		// no need to merge ranges or copy; reuse single-key set
		return wkeys
	case len(wranges) == 0 && len(wkeys) == 0:
		return nil
	case len(wranges) == 1 && len(wkeys) == 0:
		return wranges[0].Val.(watcherSet)
	}

	// copy case
	ret := make(watcherSet)
	ret.union(wg.keyWatchers[key])
	for _, item := range wranges {
		ret.union(item.Val.(watcherSet))
	}
	return ret
}
