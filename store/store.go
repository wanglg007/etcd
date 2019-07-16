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
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/etcd/pkg/types"
	"github.com/jonboulle/clockwork"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

type Store interface {
	Version() int
	Index() uint64

	Get(nodePath string, recursive, sorted bool) (*Event, error)
	Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error)
	Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error)
	Create(nodePath string, dir bool, value string, unique bool,
		expireOpts TTLOptionSet) (*Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireOpts TTLOptionSet) (*Event, error)
	Delete(nodePath string, dir, recursive bool) (*Event, error)
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error)

	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error)

	Save() ([]byte, error)
	Recovery(state []byte) error

	Clone() Store
	SaveNoCopy() ([]byte, error)

	JsonStats() []byte
	DeleteExpiredKeys(cutoff time.Time)

	HasTTLKeys() bool
}

type TTLOptionSet struct {
	ExpireTime time.Time
	Refresh    bool
}

type store struct {
	//V2存储是纯内存实现，它以树形结构将全部数据维护在内存中，树中的每个节点都是node实例。该字段记录了此树型结构的根节点
	Root           *node
	//watcherHub的主要功能是管理客户端添加的watcher监听和Event实例
	WatcherHub     *watcherHub
	//该字段是修改操作的唯一标识，每出现一次修改操作，该字段就会自增一次
	CurrentIndex   uint64
	Stats          *Stats
	CurrentVersion int
	ttlKeyHeap     *ttlKeyHeap  // need to recovery manually
	//在store进行任何操作之前，都需要获取该锁进行同步
	worldLock      sync.RWMutex // stop the world lock
	clock          clockwork.Clock
	//记录哪些节点时只读节点，这些节点都无法被修改
	readonlySet    types.Set
}

// New creates a store where the given namespaces will be created as initial directories.
func New(namespaces ...string) Store {
	s := newStore(namespaces...)
	s.clock = clockwork.NewRealClock()
	return s
}

func newStore(namespaces ...string) *store {
	s := new(store)
	s.CurrentVersion = defaultVersion
	s.Root = newDir(s, "/", s.CurrentIndex, nil, Permanent)
	for _, namespace := range namespaces {
		s.Root.Add(newDir(s, namespace, s.CurrentIndex, s.Root, Permanent))
	}
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)
	s.ttlKeyHeap = newTtlKeyHeap()
	s.readonlySet = types.NewUnsafeSet(append(namespaces, "/")...)
	return s
}

// Version retrieves current version of the store.
func (s *store) Version() int {
	return s.CurrentVersion
}

// Index retrieves the current index of the store.
func (s *store) Index() uint64 {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get returns a get event.
// If recursive is true, it will return all the content under the node path.   该方法的主要功能是在树型结构中查找指定路径对应的node节点，其中会根据recursive参数
// If sorted is true, it will sort the content by keys.                        和sorted参数决定是否加载子节点，以及是否对子节点进行排序
func (s *store) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.RLock()							//加锁
	defer s.worldLock.RUnlock()				    //方法结束时解锁

	defer func() {
		if err == nil {
			s.Stats.Inc(GetSuccess)
			if recursive {
				reportReadSuccess(GetRecursive)
			} else {
				reportReadSuccess(Get)
			}
			return
		}

		s.Stats.Inc(GetFail)
		if recursive {
			reportReadFailure(GetRecursive)
		} else {
			reportReadFailure(Get)
		}
	}()

	n, err := s.internalGet(nodePath)										//根据nodePath获取对应node节点
	if err != nil {
		return nil, err
	}

	e := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)			//创建Event实例
	e.EtcdIndex = s.CurrentIndex
	e.Node.loadInternalNode(n, recursive, sorted, s.clock)					//如果待查找节点时目录节点，则获取子节点；如果是KV节点，则加载其Value值

	return e, nil
}
// 该方法对外提供了创建node节点的功能，在其创建指定节点的过程中，还会同时创建中间不存在的目录节点(这些目录节点会被设置成永久的)。
// Create creates the node at nodePath. Create will help to create intermediate directories with no ttl.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
func (s *store) Create(nodePath string, dir bool, value string, unique bool, expireOpts TTLOptionSet) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CreateSuccess)
			reportWriteSuccess(Create)
			return
		}

		s.Stats.Inc(CreateFail)
		reportWriteFailure(Create)
	}()
	//创建目标节点，同时会创建中间涉及的目录节点
	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireOpts.ExpireTime, Create)
	if err != nil {
		return nil, err
	}

	e.EtcdIndex = s.CurrentIndex				//设置Event.EtcdIndex
	s.WatcherHub.notify(e)						//将Event添加到EventHistory中，同时触发相关的watcher

	return e, nil
}
// 该方法对外提供了创建(或更新)node节点的功能，如果指定的节点已存在，则会将其替换。该方法首先会调用internalGet()方法查找指定节点，然后根据expireOpts参数决定此次修改
// 操作的类型，之后调用internalCreate()方法替换现有节点，最后将次操作的相关信息封装成Event实例保存，并决定是否触发相关watcher实例。
// Set creates or replace the node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.Lock()								//加锁和解锁相关
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(SetSuccess)
			reportWriteSuccess(Set)
			return
		}

		s.Stats.Inc(SetFail)
		reportWriteFailure(Set)
	}()

	// Get prevNode value
	n, getErr := s.internalGet(nodePath)			//通过internalGet方法获取指定节点
	if getErr != nil && getErr.ErrorCode != etcdErr.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	if expireOpts.Refresh {							//根据此次操作类型，决定最终的节点值
		if getErr != nil {							//异常处理，不会忽略EcodeKeyNotFound异常，因为节点都没找到，所以无法获取其原值
			err = getErr
			return nil, err
		} else {
			value = n.Value							//如果是Refresh类型的修改操作，则不会改变节点的值
		}
	}
	// 通过internalCreate()方法创建指定的节点
	// Set new value
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireOpts.ExpireTime, Set)
	if err != nil {
		return nil, err
	}
	e.EtcdIndex = s.CurrentIndex

	// Put prevNode into event
	if getErr == nil {								//设置此次修改操作对应Event的PrevNode字段，将其指向修改之前的节点
		prev := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		prev.Node.loadInternalNode(n, false, false, s.clock)
		e.PrevNode = prev.Node
	}

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)						//如果此次修改操作不是Refresh类型的，则触发相关的watcher
	} else {										//此次修改操作为Refresh类型，则不会触发任何watcher，而只是将其添加到EventHistory中保存
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}
//该方法对外提供了CAS操作，其主要流程是先查找待处理节点，然后比较节点的当前值与传入的prevValue，同时会比较当前节点的ModifiedIndex与传入的prevIndex，如果相等
//则表示当前节点没有被修改过，此时就会对节点的值进行修改；如果不相等则表示当前节点已经被别人修改过，此时不应该对其进行修改。
//prevValue参数:调用者认为目标节点当前值应该是prevValue，如果当前节点被修改过，则不再是prevValue，则调用者不能对其进行修改；prevIndex同理。
func (s *store) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
	value string, expireOpts TTLOptionSet) (*Event, error) {

	var err *etcdErr.Error

	s.worldLock.Lock()					//加锁和解锁相关代码
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndSwapSuccess)
			reportWriteSuccess(CompareAndSwap)
			return
		}

		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndSwap)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {			//检测待删除节点是否为只读的
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	n, err := s.internalGet(nodePath)				//调用internalGet()方法获取待处理的节点
	if err != nil {
		return nil, err
	}
	if n.IsDir() { // can only compare and swap file  检测目标节点是否为目录节点，CAS操作只能修改KV节点
		err = etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, s.CurrentIndex)
		return nil, err
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.  比较目标节点的值和prevValue，同时也会比较当前节点的ModifiedIndex和prevIndex
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)				 //如果任意一个字段不相等，则无法更新，返回异常
		err = etcdErr.NewError(etcdErr.EcodeTestFailed, cause, s.CurrentIndex)
		return nil, err
	}
	//若上述两个完全相等，则继续后面的修改操作
	if expireOpts.Refresh {						//如果此次更新是Refresh类型，则节点的值不变
		value = n.Value
	}

	// update etcd index						递增store.CurrentIndex
	s.CurrentIndex++							//创建此次CAS操作对应的Event实例，并更新其字段

	e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	// if test succeed, write the value
	n.Write(value, s.CurrentIndex)				//更新节点值
	n.UpdateTTL(expireOpts.ExpireTime)			//更新节点的过期时间

	// copy the value for safety
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	if !expireOpts.Refresh {					//根据更新操作是否为Refresh类型，决定是否触发相应的watcher
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}
// 该方法对外提供了删除指定node节点的功能。如果待删除节点时目录节点，则需要将recursive参数设置为true，才能递归删除其子节点，并最终删除该目录节点。
// Delete deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.Lock()									//加锁与解锁相关代码
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(DeleteSuccess)
			reportWriteSuccess(Delete)
			return
		}

		s.Stats.Inc(DeleteFail)
		reportWriteFailure(Delete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {				//检测待删除节点是否为只读的
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// recursive implies dir
	if recursive {
		dir = true
	}

	n, err := s.internalGet(nodePath)					//调用internalGet()方法获取待删除的节点
	if err != nil { // if the node does not exist, return error
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	e := newEvent(Delete, nodePath, nextIndex, n.CreatedIndex)			//创建此次删除操作对应的Event实例
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if n.IsDir() {
		eNode.Dir = true
	}

	callback := func(path string) { // notify function					回调函数
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}
	//删除指定节点，如果是目录节点，则会根据recursive参数决定是否删除子节点，并在删除过程中调用回调函数，其中会触发相关watcher
	err = n.Remove(dir, recursive, callback)
	if err != nil {
		return nil, err
	}

	// update etcd index					递增store.CurrentIndex值
	s.CurrentIndex++

	s.WatcherHub.notify(e)					//触发相关watcher

	return e, nil
}

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndDeleteSuccess)
			reportWriteSuccess(CompareAndDelete)
			return
		}

		s.Stats.Inc(CompareAndDeleteFail)
		reportWriteFailure(CompareAndDelete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() { // can only compare and delete file
		return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		return nil, etcdErr.NewError(etcdErr.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(false, false, callback)
	if err != nil {
		return nil, err
	}

	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))
	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}
	// WatcherHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk walks all the nodePath and apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *node, component string) (*node, *etcdErr.Error)) (*node, *etcdErr.Error) {
	components := strings.Split(nodePath, "/")

	curr := s.Root													//从Root节点开始查找
	var err *etcdErr.Error

	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // ignore empty string			忽略空路径
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])					//查找curr节点的指定子节点
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}
// 该方法对外提供了更新node节点的功能。如果待更新节点是一个键值对节点，那么可以同时更新其Value值和过期时间。如果待更新的节点是一个目录节点，那么只能更新其过期时间。
// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error) {
	var err *etcdErr.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(UpdateSuccess)
			reportWriteSuccess(Update)
			return
		}

		s.Stats.Inc(UpdateFail)
		reportWriteFailure(Update)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))									//整理路径
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {													//检测待更新节点是否为只读的
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	n, err := s.internalGet(nodePath)														//调用internalGet()方法，查找待更新节点
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() && len(newValue) != 0 {													//检测待更新节点是否为目录节点，如果是，则不能更新其Value
		// if the node is a directory, we cannot update value to non-empty
		return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, currIndex)
	}

	if expireOpts.Refresh {					//如果此次操作是Refresh类型的，则其Value值不会更新，只会更新其过期时间
		newValue = n.Value
	}

	e := newEvent(Update, nodePath, nextIndex, n.CreatedIndex)		//创建此次操作对应的Event实例，并更新相关字段
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	n.Write(newValue, nextIndex)									//更新节点的Value值和ModifiedIndex字段

	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	n.UpdateTTL(expireOpts.ExpireTime)								//更新节点的过期时间和对应Event实例中的TTL

	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	if !expireOpts.Refresh {										//此次修改操作不是Refresh类型，则触发相关watcher
		s.WatcherHub.notify(e)
	} else {														//此次修改操作是Refresh类型，则不会触发相关watcher，只是将其添加到EventHistory中
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	s.CurrentIndex = nextIndex										//更新store.CurrentIndex字段

	return e, nil
}
//该方法是创建节点的核心。参数介绍如下：nodePath:待创建节点的完整路径；dir:此次创建的节点是否为目录节点；value:如果此次创建的节点为键值对节点，则value为其值；
//unique:是否要创建一个唯一节点；replace:待创建的节点已存在，是否要对其进行替换；expireTime:待创建节点的过期时间；
func (s *store) internalCreate(nodePath string, dir bool, value string, unique, replace bool,
	expireTime time.Time, action string) (*Event, *etcdErr.Error) {

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1
	//根据unique参数决定是否创建唯一节点，其方式在指定路径下，以nextIndex为名称创建一个节点
	if unique { // append unique item under the node path
		nodePath += "/" + fmt.Sprintf("%020s", strconv.FormatUint(nextIndex, 10))
	}

	nodePath = path.Clean(path.Join("/", nodePath))								//整理路径

	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {												//检测待创建节点是否在只读路径下，如果是，则抛出异常
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", currIndex)
	}

	// Assume expire times that are way in the past are
	// This can occur when the time is serialized to JS
	if expireTime.Before(minExpireTime) {												//设置节点的过期时间
		expireTime = Permanent
	}
	//切分路径得到父节点路径及待创建节点的名称
	dirName, nodeName := path.Split(nodePath)
	//遍历整个路径，如果路径中有目录节点不存在，则创建该目录节点。注意，这里的返回值是待创建节点的父节点，例如，待创建节点时"/foo/bar/tom"，则此返回值
	//"/foo/bar"节点
	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dirName, s.checkDir)

	if err != nil {
		s.Stats.Inc(SetFail)
		reportWriteFailure(action)
		err.Index = currIndex
		return nil, err
	}

	e := newEvent(action, nodePath, nextIndex, nextIndex)								//创建此次操作对应的Event实例
	eNode := e.Node

	n, _ := d.GetChild(nodeName)														//查找待创建节点

	// force will try to replace an existing file
	if n != nil {			//如果待创建节点已经存在，则根据replace参数决定是否替换已存在的节点
		if replace {
			if n.IsDir() {
				return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevNode = n.Repr(false, false, s.clock)				//将已存在节点的信息记录到Event.PrevNode中

			n.Remove(false, false, nil)						//删除已存在的节点
		} else {																		//如果不替换已存在的节点，则直接返回异常
			return nil, etcdErr.NewError(etcdErr.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if !dir { // create file															//根据dir参数决定创建KV节点还是目录节点
		// copy the value for safety
		valueCopy := value
		eNode.Value = &valueCopy

		n = newKV(s, nodePath, value, nextIndex, d, expireTime)

	} else { // create directory														//创建目录节点
		eNode.Dir = true

		n = newDir(s, nodePath, nextIndex, d, expireTime)
	}

	// we are sure d is a directory and does not have the children with name n.Name
	d.Add(n)																			//将创建好的节点添加到父节点中

	// node with TTL
	if !n.IsPermanent() {																//如果新建节点时非永久节点，则将其记录到ttlKeyHeap中
		s.ttlKeyHeap.push(n)

		eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)						//更新Event中的过期时间和存活时间
	}

	s.CurrentIndex = nextIndex															//递增CurrentIndex

	return e, nil
}
// 该方法会根据给定的路径从Root节点逐层查找，直至查找到目标node节点。
// InternalGet gets the node of the given nodePath.
func (s *store) internalGet(nodePath string) (*node, *etcdErr.Error) {
	nodePath = path.Clean(path.Join("/", nodePath))								//整理路径格式
	//定义walkFunc()方法，其功能是在parent节点下查找指定子节点，若查找失败，则返回异常
	walkFunc := func(parent *node, name string) (*node, *etcdErr.Error) {

		if !parent.IsDir() {															//检测parent节点是否为目录节点
			err := etcdErr.NewError(etcdErr.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]												//查找指定的子节点并返回，查找失败，则返回异常
		if ok {
			return child, nil
		}

		return nil, etcdErr.NewError(etcdErr.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}
	//真正执行walkFunc()方法，逐层查找节点的地方
	f, err := s.walk(nodePath, walkFunc)

	if err != nil {
		return nil, err
	}
	return f, nil
}
// 该方法的主要功能是删除过期的节点。该方法通过其维护的最小堆来按序清理过期节点。
// DeleteExpiredKeys will delete all expired keys
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()								//加锁和解锁相关代码
	defer s.worldLock.Unlock()

	for {
		node := s.ttlKeyHeap.top()					//获取过期时间最近的节点
		if node == nil || node.ExpireTime.After(cutoff) {
			break									//整个存储中都没有过期节点，则直接结束
		}

		s.CurrentIndex++							//递增store.CurrentIndex
		e := newEvent(Expire, node.Path, s.CurrentIndex, node.CreatedIndex)		//创建此次操作对应的Event实例
		e.EtcdIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false, s.clock)
		if node.IsDir() {														//检测过期节点是否为目录节点
			e.Node.Dir = true
		}

		callback := func(path string) { // notify function						回调函数，负责触发相关的watcher
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		s.ttlKeyHeap.pop()														//将过期节点从ttlKeyHeap中删除
		node.Remove(true, true, callback)						//将过期节点从store中删除(如果是目录节点，则递归删除其子节点)

		reportExpiredKey()
		s.Stats.Inc(ExpireCount)

		s.WatcherHub.notify(e)													//触发相关watcher
	}

}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (s *store) checkDir(parent *node, dirName string) (*node, *etcdErr.Error) {
	node, ok := parent.Children[dirName]											//在父节点中查找指定的子节点

	if ok {
		if node.IsDir() {															//如果自己子节点是目录节点，则返回子节点
			return node, nil
		}
		//如果子节点不是目录节点，则直接返回异常，因为无法在此节点下新建其他节点
		return nil, etcdErr.NewError(etcdErr.EcodeNotDir, node.Path, s.CurrentIndex)
	}
	//如果没有查找到对应节点，则创建对应的目录节点，并添加到父节点中
	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, Permanent)

	parent.Children[dirName] = n

	return n, nil																	//返回的依然是子节点
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node. Or there will
// be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	b, err := json.Marshal(s.Clone())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) SaveNoCopy() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Clone() Store {
	s.worldLock.Lock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	s.worldLock.Unlock()
	return clonedStore
}

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring go routines.
func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	err := json.Unmarshal(state, s)

	if err != nil {
		return err
	}

	s.ttlKeyHeap = newTtlKeyHeap()

	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) HasTTLKeys() bool {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.ttlKeyHeap.Len() != 0
}
