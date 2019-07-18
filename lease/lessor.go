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

package lease

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/lease/leasepb"
	"github.com/coreos/etcd/mvcc/backend"
)

// NoLease is a special LeaseID representing the absence of a lease.
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	leaseBucketName = []byte("lease")

	// maximum number of leases to revoke per second; configurable for tests
	leaseRevokeRate = 1000

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	SetRangeDeleter(rd RangeDeleter)

	// Grant grants a lease that expires at least after TTL seconds.		创建Lease实例，该Lease会在指定的时间(ttl)之后过期
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the		撤销指定的Lease，该Lease实例相关的LeastItem也会被删除
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	Revoke(id LeaseID) error

	// Attach attaches given leaseItem to the lease with given LeaseID.		将指定Lease与指定LeastItem绑定，在LeaseItem中封装了键值对的Key值
	// If the lease does not exist, an error will be returned.
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.								根据LeaseItem查询对应Lease实例的id
	// If no lease found, NoLease value will be returned.
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.	取消指定Lease与指定LeastItem之间的绑定关系
	// If the lease does not exist, an error will be returned.
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages		如果当前节点成为Leader节点，则其使用的Lessor实例将通过该方法晋升，成为主
	// the expiration and renew of leases.												Lessor。主Lessor将控制着整个集群中所有Lease实例
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.				如果当前节点从Leader状态转换为其他状态，则会通过该方法将其使用的Lessor实例进行降级
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
	// an error will be returned.
	Renew(id LeaseID) (int64, error)										//续约指定的Lease实例

	// Lookup gives the lease at a given lease id, if any					查找指定id对应的Lease实例
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.		如果出现lease实例过期，则会被写入到ExpiredLeaseC通道中
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.	从底层的V3存储中恢复Lease实例
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
type lessor struct {
	mu sync.Mutex

	// demotec is set when the lessor is the primary.		用于判断当前Lessor实例是否为主lessor(primary lessor)。如果当前lessor是主Lessor实例，则会开启
	// demotec will be closed if the lessor is demoted.		该通道，当节点切换成非Leader状态时，会关闭给通道。
	demotec chan struct{}

	// TODO: probably this should be a heap with a secondary
	// id index.
	// Now it is O(N) to loop over the leases to find expired ones.				记录id到Lease实例之间的映射。
	// We want to make Grant, Revoke, and findExpiredLeases all O(logN) and
	// Renew O(1).
	// findExpiredLeases and Renew should be the most frequent operations.
	leaseMap map[LeaseID]*Lease

	itemMap map[LeaseItem]LeaseID												//记录LeaseItem到Lease实例id的映射

	// When a lease expires, the lessor will delete the							RangeDeleterious接口主要用于从底层的存储中删除过期的Lease实例
	// leased range (or key) by the RangeDeleter.
	rd RangeDeleter

	// backend to persist leases. We only persist lease ID and expiry for now.	底层持久化Lease的存储
	// The leased items can be recovered by iterating all the keys in kv.
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	minLeaseTTL int64															//Lease实例过期时间的最小值

	//过期的Lease实例会被写入该通道中，并等待其他goroutine进行处理
	expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	doneC chan struct{}
}

func NewLessor(b backend.Backend, minLeaseTTL int64) Lessor {
	return newLessor(b, minLeaseTTL)
}
//该函数会创建Lessor实例，并调用initAndRecover方法完成初始化操作，同时还会启动一个后台goroutine查找当前Lessor中是否存在过期的lease实例。
func newLessor(b backend.Backend, minLeaseTTL int64) *lessor {
	l := &lessor{
		leaseMap:    make(map[LeaseID]*Lease),
		itemMap:     make(map[LeaseItem]LeaseID),
		b:           b,
		minLeaseTTL: minLeaseTTL,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
	}
	l.initAndRecover()

	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}
//该方法会根据指定的id和过期时长新建Lease实例，然后将其保存到leaseMap中并进行持久化。
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	l := &Lease{										//新建Lease实例
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	if _, ok := le.leaseMap[id]; ok {					//检测指定的id在当前leaseMap中是否存在对应Lease实例
		return nil, ErrLeaseExists
	}

	if l.ttl < le.minLeaseTTL {							//修正过期时长
		l.ttl = le.minLeaseTTL
	}

	if le.isPrimary() {
		l.refresh(0)							//更新Lease实例的过期时间戳
	} else {
		l.forever()										//如果当前节点不是Leader，则将Lease设置为永不过期
	}

	le.leaseMap[id] = l									//在leaseMap中记录该Lease实例
	l.persistTo(le.b)									//持久化Lease信息

	return l, nil
}
//该方法负责撤销指定id对应的Lease实例，其中会关闭Lease实例对应的revokec通道，并将Lease实例从lessor.leaseMap和存储中删除。需要注意的是，除了删除Lease实例
//本身，还需要删除与该Lease实例关联的键值对。
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	l := le.leaseMap[id]					//从leaseMap中查找指定的Lease实例
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	defer close(l.revokec)					//在方法结束时，关闭该Lease实例中的revokec通道
	// unlock before doing external work
	le.mu.Unlock()

	if le.rd == nil {
		return nil
	}

	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backened hashes will be different
	keys := l.Keys()						//获取与该Lease实例绑定的Key值
	sort.StringSlice(keys).Sort()
	for _, key := range keys {
		txn.DeleteRange([]byte(key), nil)			//删除与该Lease实例绑定的全部键值对
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.leaseMap, l.ID)				//将该Lease实例从leaseMap中删除
	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	le.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)))		//将该Lease实例的信息从底层的V3存储中删除

	txn.End()								//提交事务
	return nil
}
// 该方法负责续租一个已经存在的Lease实例，当然，只有当前节点是Leader时才能完成该续租操作。
// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.Lock()

	unlock := func() { le.mu.Unlock() }
	defer func() { unlock() }()

	if !le.isPrimary() {				//检测当前lessor是否为主Lessor实例
		// forward renew request to primary instead of returning error.
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]				//在leaseMap中查找指定的Lease实例
	if l == nil {
		return -1, ErrLeaseNotFound
	}
	//如果指定Lease不存在，则返回错误信息
	if l.expired() {					//检测Lease实例是否过期
		le.mu.Unlock()
		unlock = func() {}
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec:
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	l.refresh(0)			//更新过期时间
	return l.ttl, nil
}

func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.leaseMap[id]
}

func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	sort.Sort(leasesByExpiry(leases))
	return leases
}

func (le *lessor) Leases() []*Lease {
	le.mu.Lock()
	ls := le.unsafeLeases()
	le.mu.Unlock()
	return ls
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		l.refresh(extend)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		return
	}

	// adjust expiries in case of overlap
	leases := le.unsafeLeases()

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever()
	}

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}
// 该方法负责将指定的Lease实例和键值对绑定。
// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]				//从leaseMap中查找指定的Lease实例
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{}		//在itemSet这个map中记录该LeaseItem实例
		le.itemMap[it] = id				//在Lease实例的itemMap字段中记录该LeaseItem实例，从而实现绑定
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.Lock()
	id := le.itemMap[item]
	le.mu.Unlock()
	return id
}
// 该方法负责解绑指定的Lease实例和键值对解绑。
// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}
//该方法会根据当前Lessor是否为主Lessor，决定是否检测过期Lease实例。
func (le *lessor) runLoop() {
	defer close(le.doneC)

	for {
		var ls []*Lease

		// rate limit
		revokeLimit := leaseRevokeRate / 2				//如果过期的Lease实例过多，则进行限流

		le.mu.Lock()
		if le.isPrimary() {								//检测当前的Lessor是否为主Lessor
			ls = le.findExpiredLeases(revokeLimit)		//查询
		}
		le.mu.Unlock()

		if len(ls) != 0 {
			select {
			case <-le.stopC:
				return
			case le.expiredC <- ls:						//将过期的Lease实例集合
			default:									//如果当前expiredC通道阻塞，则放弃处理检测
				// the receiver of expiredC is probably busy handling
				// other stuff
				// let's try this next time after 500ms
			}
		}

		select {
		case <-time.After(500 * time.Millisecond):		//阻塞500毫秒，再开始下次检测
		case <-le.stopC:
			return
		}
	}
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit	该方法会遍历leaseMap中全部的Lease实例，并调用Lease.expired方法检测是否过期。
// and returns the expired leases that needed to be revoked.
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)						//记录过期的Lease实例

	for _, l := range le.leaseMap {
		// TODO: probably should change to <= 100-500 millisecond to
		// make up committing latency.
		if l.expired() {								//检测该Lease实例是否过期，其中通过当前时间与Lease.expiry进行比较
			leases = append(leases, l)

			// reach expired limit
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx()						//开启v3存储的读写事务
	tx.Lock()

	tx.UnsafeCreateBucket(leaseBucketName)		//创建lease Bucket
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)				//查找lease Bucket中全部信息
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])				//反序列化得到leasepb.Lease实例，其中只记录了对应的id和过期时间
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {			//修正过期时间
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{				//创建Lease实例并记录到leaseMap中
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}
	tx.Unlock()

	le.b.ForceCommit()							//提交事务
}

type Lease struct {
	ID  LeaseID												//该Lease实例的唯一标识
	ttl int64 // time to live in seconds					//该Lease实例的存活时长
	// expiryMu protects concurrent accesses to expiry
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	expiry time.Time										//该Lease实例过期的时间戳

	// mu protects concurrent accesses to itemSet
	mu      sync.RWMutex
	itemSet map[LeaseItem]struct{}							//该map中的Key是与当前Lease实例绑定的LeaseItem实例，Value始终为空结构体
	revokec chan struct{}									//该Lease实例被撤销时会关闭该通道，从而实现通知监听该通道的goroutine的效果
}

func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) persistTo(b backend.Backend) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: int64(l.ttl)}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val)
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// refresh refreshes the expiry of the lease.
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.ttl)*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// Keys returns all the keys attached to the lease.
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}
