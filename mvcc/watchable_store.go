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
	"sync"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// non-const so modifiable by tests
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// TODO: find a good buf value. 1024 is just a random one that
	// seems to be reasonable.
	chanBufLen = 1024

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}
//该结构体完成了注册watcher实例、管理watcher实例，以及发送触发watcher之后的响应等核心功能。
type watchableStore struct {
	*store

	// mu protects watcher groups and batches. It should never be locked	在修改synced、unsynced等字段时，需要获取该锁进行同步
	// before locking store.mu to avoid deadlock.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	// 如果watcher实例关联的ch通道被阻塞，则对应的watcherBatch实例会暂时记录到该字段中。在watchableStore实例中还会启动一个后台goroutine来处理该字段中保存的
	// watcherBatch实例。
	victims []watcherBatch
	// 当有新的watcherBatch实例添加到victims字段中时，会向该通道中发送一个空结构体作为信号
	victimc chan struct{}

	// contains all unsynced watchers that needs to sync with events that have happened
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	// synced中全部的watcher实例都已经同步完毕，并等待新的更新操作；synced中的watcher实例都落后于当前最新更新操作，并且有一个单独的后台goroutine帮助其进行追赶
	// 当etcd服务端收到客户端的watch请求时，如果请求携带了revision参数，则比较该请求的revision信息和store.currentRev信息：如果请求的revision信息较大，则放入
	// synced中，否则翻入unsynced红。watchableStore实例会启动一个一个后台的goroutine持续同步unsynced，然后将完成同步的watcher实例迁移到synced中储存。
	synced watcherGroup

	stopc chan struct{}
	//在watchableStore实例中会启动两个后台goroutine，在watchableStore.Close()方法中会通过该sync.WaitGroup实例实现等待两个后台goroutine执行完成的功能
	wg    sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) ConsistentWatchableKV {
	return newWatchableStore(b, le, ig)
}
//该方法完成watchableStore实例的初始化，其中会启动上面提到的两个后台goroutine。
func newWatchableStore(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) *watchableStore {
	s := &watchableStore{
		store:    NewStore(b, le, ig),				//创建store实例
		victimc:  make(chan struct{}, 1),
		unsynced: newWatcherGroup(),				//初始化unsynced watcherGroup
		synced:   newWatcherGroup(),				//初始化synced watcherGroup
		stopc:    make(chan struct{}),
	}
	s.store.ReadView = &readView{s}				//创建readView和writeView实例
	s.store.WriteView = &writeView{s}
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write() })
	}
	s.wg.Add(2)								//初始化sync.WaitGroup实例
	go s.syncWatchersLoop()							//启动处理unsynced watcherGroup的后台goroutine
	go s.syncVictimsLoop()							//启动处理victims的后台goroutine
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}
//
func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{						//创建watcher实例
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()														//加mu锁，因为要读取currentRev字段，所以还需要加revMu锁
	s.revMu.RLock()
	synced := startRev > s.store.currentRev || startRev == 0		//比较startRev与store.currentRev，决定待添加的watcher实例是否已同步完成
	if synced {
		wa.minRev = s.store.currentRev + 1							//设置待添加watcher的minRev字段值
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
	}
	//如果待添加watcher已同步完成，则将其添加到synced watcherGroup中，否则添加到unsynced watcherGroup中
	if synced {
		s.synced.add(wa)
	} else {
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	watcherGauge.Inc()
	//返回该watcher实例，以及取消该watcher实例的回调函数
	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()										//加锁
		if s.unsynced.delete(wa) {						//尝试从unsynced watcherGroup中删除该watcher实例
			slowWatcherGauge.Dec()
			break
		} else if s.synced.delete(wa) {				//尝试从synced watcherGroup中删除该watcher实例
			break
		} else if wa.compacted {						//如果待删除的watcher实例已经因为压缩操作而删除，则直接返回
			break
		} else if wa.ch == nil {
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			panic("watcher not victim but not in watch groups")
		}
		//如果在(un)synced watcherGroup中都没有，该watcher可能已经被触发了，则在victims中查找
		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {							//查找待删除watcher对应的watcherBatch实例
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			slowWatcherGauge.Dec()						//删除victims字段中对应的watcher实例
			delete(victimBatch, wa)
			break
		}
		// 如果未找到，可能是待删除的watcher刚刚从synced watcherGroup中删除且未添加到victims中，所以稍后进行重试
		// victim being processed so not accessible; retry
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	watcherGauge.Dec()
	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}
// 该goroutine的主要作用是每隔100ms对unsynced watcherGroup进行一次批量的同步。syncWatchers()方法是批量同步unsynced watcherGroup的核心，该方法步骤如下：
// (1)从unsynced watcherGroup中选择一批watcher实例，作为此次需要进行同步的watcher实例；(2)从该批watcher实例中查找最小的minRev字段值；(3)在BoltDB中进行
// 范围查询，查询minRev~currentRev(当前revision值)的所有键值对；(4)过滤掉该批watcher实例中因minRev~currentRev之间发生压缩操作而被删除的watcher实例;(5)
// 遍历步骤3中查询到的键值对，将其中的更新操作转换为Event事件，然后封装成WatchResponse，并写入对应的watcher.ch通道中。如果watcher.ch通道被填充满，则将
// Event事件记录到watchableStore.victims中，并由另一个后台goroutine处理该字段中积累的Event事件；(6)将已经完成同步的watcher实例记录到synced watcherGroup
// 中，同时将其中unsynced watcherGroup中删除。
// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()								//该后台goroutine结束时调用

	for {
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size()	//获取当前的unsynced watcherGroup的大小
		s.mu.RUnlock()

		unsyncedWatchers := 0
		if lastUnsyncedWatchers > 0 {
			//存在需要进行同步的watcher实例，则调用syncWatchers()方法对unsynced watcherGroup中的watcher进行批量同步
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			waitDuration = syncDuration
		}
		//调整该后台goroutine执行的时间间隔
		select {							//等待waitDuration时长之后，再开始下次同步
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}
// 在初始化watchableStore实例时启动的另一个后台goroutine会执行syncVictimsLoop()方法，在该方法中会定期处理watchableStore.victims中缓存的watcherBatch实例。
// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()								//该后台goroutine结束时调用

	for {
		for s.moveVictims() != 0 {					//循环处理victims中缓存的watcherBatch实例
			// try to update all victim watchers
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		var tickc <-chan time.Time
		if !isEmpty {
			tickc = time.After(10 * time.Millisecond)
		}
		//根据victims的长度决定下次处理victims的时间间隔
		select {
		case <-tickc:
		case <-s.victimc:
		case <-s.stopc:
			return
		}
	}
}
// 该方法会遍历victims字段中记录的watchBatch实例，并尝试将其中的Event实例封装成WatchResponse重新发送。如果发送失败，则将其放回victims字段中保存，等待下一次重试；
// 如果发送成功，则根据相应的watcher的同步情况，将watcher实例迁移到(un)synced watcherGroup中。
// moveVictims tries to update watches with already pending event data
func (s *watchableStore) moveVictims() (moved int) {
	s.mu.Lock()
	victims := s.victims
	s.victims = nil									//清空victims字段
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims {					//遍历victims中记录的watcherBatch
		// try to send responses again
		for w, eb := range wb {					//从watcherBatch中获取eventBatch实例
			// watcher has observed the store up to, but not including, w.minRev
			rev := w.minRev - 1
			//将eventBatch封装成watchResponse，并调用watcher.send()方法尝试将其发送出去
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))
			} else {								//如果watcher.cn通道依然阻塞，则将对应的Event实例重新放回newVictim变量中保存
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			moved++									//记录重试成功的eventBatch个数
		}

		// assign completed victim watchers to unsync/sync
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		for w, eb := range wb {					//遍历watcherBatch
			//如果eventBatch实例被记录到newVictim中，则表示watcher.cn通道依然阻塞，WatchResponse发送失败
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				continue
			}
			//如果eventBatch实例未被记录到newVictim中，则表示watchResponse发送成功
			w.victim = false
			if eb.moreRev != 0 {					//检测当前watcher后续是否还有未同步的Event事件
				w.minRev = eb.moreRev
			}
			if w.minRev <= curRev {					//当前watcher未完成同步，移动到unsynced watcherGroup中
				s.unsynced.add(w)
			} else {								//当前watcher已经完成同步,移动到synced watcherGroup中
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	if len(newVictim) > 0 {							//使用newVictim更新watchableStore.victims字段
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	return moved									//返回重试成功的eventBatch个数
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsynced.size() == 0 {						//如果unsynced watcherGroup为空，则直接返回
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev
	//从unsynced watcherGroup中查找一批此次需要同步的watcher实例，并将这些watcher实例封装成watcherGroup实例返回，返回的minRev是这批待同步的
	//watcher实例中minRev字段最小值
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	minBytes, maxBytes := newRevBytes(), newRevBytes()				//将minRev和currentRev写入minBytes和maxBytes中
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	tx := s.store.b.ReadTx()										//获取只读事务
	tx.Lock()
	//调用UnsafeRange()方法，对key Bucket进行范围查找
	revs, vs := tx.UnsafeRange(keyBucketName, minBytes, maxBytes, 0)
	//将查询到的键值对转换成对应的Event事件
	evs := kvsToEvents(wg, revs, vs)
	tx.Unlock()

	var victims watcherBatch
	wb := newWatcherBatch(wg, evs)			//将上述watcher集合及Event事件封装成watcherBatch
	for w := range wg.watchers {
		w.minRev = curRev + 1				//更新watcher.minRev字段，这样就不会被同一个修改操作触发两次

		eb, ok := wb[w]
		if !ok {
			// bring un-notified watcher to synced
			// 该watcher实例所监听的键值对没有更新操作(在minRev~currentRev之间)，所以没有被触发，故同步完毕，这里会将其转移到unsynced watcherGroup中
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		if eb.moreRev != 0 {
			// 在minRev~currentRev之间触发当前watcher的更新操作过多，无法全部放到一个eventBatch中，这里只将该watcher.minRev设置成moreRev，则下次处理unsynced
			// watcherGroup时，会将其后的更新操作查询处理继续处理
			w.minRev = eb.moreRev
		}
		//将前面创建的Event事件集合封装成WatchResponse实例，然后写入watcher.ch通道中
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {				//如果watcher.cn通道阻塞，则将该watcher.victim字段设置为true
			if victims == nil {
				victims = make(watcherBatch)		//初始化victims变量
			}
			w.victim = true
		}

		if w.victim {			//如果watcher.cn通道阻塞，则将触发它的Event事件记录到victims中
			victims[w] = eb
		} else {
			//如果后续还有其他未处理的Event事件，则当前watcher依然未同步完成
			if eb.moreRev != 0 {
				// stay unsynced; more to read
				continue
			}
			s.synced.add(w)		//当前watcher已经同步完成，加入synced watcherGroup中
		}
		s.unsynced.delete(w)	//将当前watcher从unsynced wtcherGroup中删除
	}
	//将victims变量中记录watcherBatch添加到watchableStore.victims中，等待处理
	s.addVictim(victims)

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	return s.unsynced.size()	//返回此次处理之后unsynced watcherGroup的长度
}
// 该函数负责将前面从BoltDB中查询到的键值对信息转换成相应的Event实例。
// kvsToEvents gets all events for the watchers from all key-value pairs
func kvsToEvents(wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals {						//键值对数据
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {		//反序列化得到KeyValue实例
			plog.Panicf("cannot unmarshal event: %v", err)
		}
		//在此批处理的所有watcher实例中，是否有监听了该Key的实例。需要注意的是，unsynced watcherGroup中可能同时记录了监听单个Key的watcher和监听范围的watcher，所以
		//在watcherGroup.contain()方法中会同时查找这两种
		if !wg.contains(string(kv.Key)) {
			continue
		}

		ty := mvccpb.PUT							//默认是更新操作
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE						//如果是tombstone,则表示删除操作
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})	//将该键值对转换成对应的Event实例，并记录下来
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	//将传入的Event实例转换成watcherBatch，然后进行遍历，逐个watcher进行处理
	for w, eb := range newWatcherBatch(&s.synced, evs) {
		if eb.revs != 1 {
			plog.Panicf("unexpected multiple revisions in notification")
		}
		//将当前watcher对应的Event实例封装成watchResponse实例，并尝试写入watcher.ch通道中
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {					//如果watcher.cn通道阻塞，则将这些Event实例记录到watchableStore.victims字段中
			// move slow watcher to victims
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w)		//将该watcher从synced watcherGroup中删除
			slowWatcherGauge.Inc()
		}
	}
	s.addVictim(victim)
}

func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	s.victims = append(s.victims, victim)		//将watcherBatch实例追加到watcheableStore.victims字段中
	select {
	case s.victimc <- struct{}{}:             //向victimc通道中发送一个信号
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	//只有watcher在synced aatcherGroup中时，才会响应该方法
	if _, ok := s.synced.watchers[w]; ok {
		//注意这里发送的WatchResponse实例不包含任何Event事件，只包含当前的revision值。如果当前watcher.cn通道已经阻塞，其中阻塞的WatchResponse实例中也包含了
		//revisiion值，也可以说明当前watcher的处理进度，则没有必要继续添加空的WatchResponse实例了。
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

type watcher struct {
	// the watcher key
	key []byte											//该watcher实例监听的原始Key值
	// end indicates the end of the range to watch.		该watcher实例监听的结束位置(也是一个原始Key值)。如果该字段有值，则当前watcher实例是一个范围watcher。
	// If end is set, the watcher is on a range.        如果该字段未设置值，则当前watcher只监听上面的key字段对应的键值对。
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	victim bool											//当上述ch通道阻塞时，会将该字段设置成true

	// compacted is set when the watcher is removed because of compaction
	compacted bool										//如果该字段被设置成true，则表示当前watcher已经因为发生了压缩操作而被删除

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	restore bool

	// minRev is the minimum revision update the watcher will accept
	// 能够触发当前watcher实例的最小revision值。发生在该revision之前的更新操作是无法触发该watcher实例
	minRev int64
	id     WatchID										//当前watcher实例的唯一标识

	//过滤器。触发当前watcher实例的事件需要经过这些过滤器的过滤才能封装进响应
	fcs []FilterFunc
	// a chan to send out the watch response.			当前watcher实例被触发之后，会向该通道中写入WatchResponse。该通道可能是由多个watcher实例共享的。
	// The chan might be shared with other watchers.
	ch chan<- WatchResponse
}
//该方法会使用watcher.fcs中记录的过滤器对Event进行过滤，只有通过过滤的Event实例才能放出去。
func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0				//发送WatchResponse中封装的Event事件

	if len(w.fcs) != 0 {								//watcher.fcs字段中记录了过滤Event实例的过滤器
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events {						//遍历watcherResponse中封装的Event实例，并逐个进行过滤
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered {								//通过所有过滤器检测的Event实例才能被记录到watcherResponse中
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	//如果没有通过过滤的Event实例，则直接返回true，此次发送结束
	select {
	//将watchResponse实例写入watcher.cn通道中，如果watcher.cn通道阻塞，则通过返回false进行表示
	case w.ch <- wr:
		return true
	default:
		return false
	}
}
