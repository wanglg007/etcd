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
	"sort"
	"sync"

	"github.com/google/btree"
)

type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)		//查询指定的key
	Range(key, end []byte, atRev int64) ([][]byte, []revision)						//范围查询
	Revisions(key, end []byte, atRev int64) []revision
	Put(key []byte, rev revision)													//添加元素
	Tombstone(key []byte, rev revision) error										//添加Tombstone
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{}										//压缩BTree中的全部keyIndex
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

type treeIndex struct {
	sync.RWMutex
	tree *btree.BTree
}

func newTreeIndex() index {
	return &treeIndex{
		tree: btree.New(32),		//这里将BTree的度初始化为32，即除了根节点的每个节点至少有32个元素，每个节点最多有64个元素
	}
}
//该方法主要完成两项操作，一是向BTree中添加keyIndex实例，二是向keyIndex中追加revision信息。
func (ti *treeIndex) Put(key []byte, rev revision) {
	keyi := &keyIndex{key: key}		//创建keyIndex实例

	ti.Lock()						//加锁和解锁相关逻辑
	defer ti.Unlock()
	item := ti.tree.Get(keyi)		//通过BTree.Get()方法在BTree上查找指定的元素
	if item == nil {				//在BTree中不存在指定的key
		keyi.put(rev.main, rev.sub)	//向keyIndex中追加一个revision
		ti.tree.ReplaceOrInsert(keyi)			//通过BTree.ReplaceOrInsert()方法向BTree中添加keyIndex实例
		return
	}
	okeyi := item.(*keyIndex)		//如果在BTree中查找到该Key对应的元素，则向其中追加一个revision实例
	okeyi.put(rev.main, rev.sub)
}
//该方法负责从BTree中查询revision信息
func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	keyi := &keyIndex{key: key}		//创建keyIndex实例
	ti.RLock()						//加锁和解锁的相关逻辑
	defer ti.RUnlock()
	if keyi = ti.keyIndex(keyi); keyi == nil {			//查询指定Key对应的keyIndex实例
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	return keyi.get(atRev)			//在查询到的keyIndex实例中，查找对应的revision信息
}
//该方法通过BTree.Get()方法查询指定的元素
func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex)) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.RLock()
	defer ti.RUnlock()

	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		f(item.(*keyIndex))
		return true
	})
}

func (ti *treeIndex) Revisions(key, end []byte, atRev int64) (revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(atRev); err == nil {
			revs = append(revs, rev)
		}
	})
	return revs
}
//该方法负责查询BTree中key~end之间元素中指定revision信息
func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)				//如果未指定end参数，则只查询key对应的revision信息
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(atRev); err == nil {	//从当前元素中查询指定的revision元素，并追加到revs中
			revs = append(revs, rev)
			keys = append(keys, ki.key)						//将当前key追加到keys中，等待返回
		}
	})
	return keys, revs
}
//该方法会先在BTree中查找指定keyIndex实例，然后调用keyIndex.tombstone()方法结束其当前generation实例并创建新的generation实例。
func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)
	return ki.tombstone(rev.main, rev.sub)
}
// 该方法会查询key~end的元素，并从中查询main revision部分大于指定值的revision信息。注意，最终返回的revision实例不是按照key进行排序的，而是按照
// revision.GreaterThan()排序的。
// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}			//创建key对应的keyIndex实例

	ti.RLock()							//加锁和解锁相关逻辑
	defer ti.RUnlock()

	if end == nil {
		item := ti.tree.Get(keyi)		//如果未指定end参数，则只查询key对应的revision信息
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(rev)			//调用keyIndex.since()方法，获取rev之后的revision实例
	}

	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {		//调用BTree.AscendGreaterOrEqual()方法遍历比key大的元素
		if len(endi.key) > 0 && !item.Less(endi) {						//忽略大于end的元素
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(rev)...)						//调用keyIndex.since()方法，获取当前keyIndex实例中rev之后的revision实例
		return true
	})
	sort.Sort(revisions(revs))											//对revs中记录的revision实例进行排序，并返回

	return revs
}
//该方法会遍历BTree中所有的keyIndex实例，并调用keyIndex.compact()方法对其中的generations进行压缩。
func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	var emptyki []*keyIndex
	plog.Printf("store.index: compact %d", rev)
	// TODO: do not hold the lock for long time?
	// This is probably OK. Compacting 10M keys takes O(10ms).
	ti.Lock()													//加锁和解锁相关逻辑
	defer ti.Unlock()
	ti.tree.Ascend(compactIndex(rev, available, &emptyki))		//调用BTree.Ascend()方法遍历其中的全部keyIndex实例，在遍历过程中，会将待删除的keyIndex实例添加到emptyki中
	for _, ki := range emptyki {
		item := ti.tree.Delete(ki)
		if item == nil {
			plog.Panic("store.index: unexpected delete failure during compaction")
		}
	}
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}
//该方法完成了具体的压缩操作，并记录了generations为空的keyIndex实例(即待删除的keyIndex)。
func compactIndex(rev int64, available map[revision]struct{}, emptyki *[]*keyIndex) func(i btree.Item) bool {
	return func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.compact(rev, available)			//调用keyIndex.compact()方法进行压缩
		if keyi.isEmpty() {						//检测keyIndex实例中generations是否已为空
			*emptyki = append(*emptyki, keyi)	//将空的keyIndex实例追加到emptyki中等待删除
		}
		return true
	}
}

func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}
