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
	"fmt"

	"github.com/google/btree"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
type keyIndex struct {
	key         []byte												//客户端提供的原始Key值
	modified    revision // the main rev of the last modification	记录该Key值最后一次修改对应的revision信息
	generations []generation
}

// put puts a revision to the keyIndex.				该方法负责向keyIndex中追加新的revision信息
func (ki *keyIndex) put(main int64, sub int64) {
	rev := revision{main: main, sub: sub}			//根据传入的main reversion和sub reversion创建reversion实例

	if !rev.GreaterThan(ki.modified) {				//检测该reversion实例的合法性
		plog.Panicf("store.keyindex: put with unexpected smaller revision [%v / %v]", rev, ki.modified)
	}
	if len(ki.generations) == 0 {					//创建generations[0]实例
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 { // create a new key		新建key，则初始化对应generation实例的created字段
		keysGauge.Inc()
		g.created = rev
	}
	g.revs = append(g.revs, rev)					//向generation.revs中追加revision信息
	g.ver++											//递增generation.ver
	ki.modified = rev								//更新keyIndex.modified字段
}
//该方法恢复当前的keyIndex中的信息。
func (ki *keyIndex) restore(created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		plog.Panicf("store.keyindex: cannot restore non-empty keyIndex")
	}

	ki.modified = modified
	g := generation{created: created, ver: ver, revs: []revision{modified}}		//创建generation实例，并添加到generations中
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}
// 该方法会在当前generation中追加一个revision实例，然后新建一个generation实例。
// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
func (ki *keyIndex) tombstone(main int64, sub int64) error {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected tombstone on empty keyIndex %s", string(ki.key))
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	//检测当前的keyIndex.generations字段是否为空，以及当前使用的generation实例是否为空
	ki.put(main, sub)										//在当前generation中追加一个revision
	ki.generations = append(ki.generations, generation{})	//在generations中创建新generation实例
	keysGauge.Dec()
	return nil
}
// 该方法会在当前KeyIndex实例中查找小于指定的main revision的最大revision
// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
func (ki *keyIndex) get(atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {				//检测当前的keyIndex是否记录了generation实例
		plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
	}
	g := ki.findGeneration(atRev)	//根据给定的main revision，查找对应的genertaion实例，如果没有对应的generation，则报错
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	//在generation中查找对应的revisiion实例，如果查找失败，就会报错
	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}
	//返回值除了目标revision实例，还有该Key此次创建的revision实例，以及到目标revision之前的修改次数
	return revision{}, revision{}, 0, ErrRevisionNotFound
}
// 该方法用于批量查找revision，该方法负责返回当前keyIndex实例中main部分大于指定值的revision实例，如果查找结果中包含了多个main部分相同的revision实例，则只返回
// 其中sub部分最大的实例。
// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
func (ki *keyIndex) since(rev int64) []revision {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
	}
	since := revision{rev, 0}
	var gi int
	// 倒序遍历所有generation实例，查询从哪个generation开始查找(即gi对应的generation实例)
	// find the generations to start checking
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		//比较创建当前generation实例的revision与since 在generation.GreaterThan()方法中会先比较main revision部分，如果相同，则比较sub revision部分。
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision										//记录返回结果
	var last int64											//用于记录当前所遇到的最大的main revision值
	for ; gi < len(ki.generations); gi++ {					//从gi处开始遍历generation实例
		for _, r := range ki.generations[gi].revs {		//遍历每个generation中记录的revision实例
			if since.GreaterThan(r) {						//忽略main revision部分较小的revision实例
				continue
			}
			//如果查找到mainrevision部分相同且sub revision更大的revision实例，则用其替换之前记录的返回结果，从而实现main部分相同时只返回sub部分较大的revision实例
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)				//将符合条件的revision实例记录到revs中
			last = r.main						//更新last
		}
	}
	return revs
}
// 随着客户端不断修改键值对，keyIndex中记录的revision实例和generation实例会不断增加，可以通过调用compact()方法对keyIndex进行压缩。在压缩时会将main部分小于指定
// 值的全部revision实例全部删除。在压缩过程中，如果出现了空的generation实例，则会将其删除。如果keyIndex中全部的generation实例都被清除了，则该KeyIndex实例也会
// 被删除。
// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
func (ki *keyIndex) compact(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected compact on empty keyIndex %s", string(ki.key))
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	if !g.isEmpty() {						//遍历目标generation中的全部revision实例，清空目标generation实例中main部分小于指定值的revision
		// remove the previous contents.
		if revIndex != -1 {
			g.revs = g.revs[revIndex:]		//清理目标generation
		}
		// remove any tombstone				如果目标generation实例中只有tombstone，则将其删除
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++						//递增genIdx，后面会清理genIdx之前的全部generation实例
		}
	}

	// remove the previous generations.		清理目标generation实例之前的全部generation实例
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev revision) bool {					//后面遍历generation时使用的回调函数
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	for genIdx < len(ki.generations)-1 {			//遍历所有的generation实例，目标generation实例中的tombstone大于指定的revision
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg										//指向当前keyIndex实例中最后一个generation实例，并逐个向前查找

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 {		//过滤调用空的generation实例
			cg--
			continue
		}
		g := ki.generations[cg]
		if cg != lastg {							//如果不是最后一个generation实例，则先与tombone revision进行比较
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		if g.revs[0].main <= rev {					//与generation中的第一个revision比较
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (a *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*keyIndex).key) == -1
}

func (a *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(a.key, b.key) {
		return false
	}
	if a.modified != b.modified {
		return false
	}
	if len(a.generations) != len(b.generations) {
		return false
	}
	for i := range a.generations {
		ag, bg := a.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
type generation struct {
	//记录当前generation所含的修改次数，即revs数组的长度
	ver     int64
	created revision // when the generation is created (put in first revision).		//记录创建当前generation实例时对应的revision信息
	//当客户端不断更新该键值对时，revs数组会不断追加每次更新对应revision信息
	revs    []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }
// 该方法查找从该generation实例中查找符合条件的revision实例
// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])				//逆序查找generation.revs数组
		if !ok {
			return l - i - 1				//返回目标revision的下标
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (a generation) equal(b generation) bool {
	if a.ver != b.ver {
		return false
	}
	if len(a.revs) != len(b.revs) {
		return false
	}

	for i := range a.revs {
		ar, br := a.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
