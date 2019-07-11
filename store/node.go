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
	"path"
	"sort"
	"time"

	etcdErr "github.com/coreos/etcd/error"
	"github.com/jonboulle/clockwork"
)

// explanations of Compare function result
const (
	CompareMatch = iota
	CompareIndexNotMatch
	CompareValueNotMatch
	CompareNotMatch
)

var Permanent time.Time

// node is the basic element in the store system.  结构体node是V2版本存储中最基本的元素，V2版本存储是树形结构的，node既可以表示其中的一个键值对(叶子节点)，
// A key-value pair will have a string value       也可以表示一个目录(非叶子节点)。
// A directory will have a children map
type node struct {
	Path string					//当前节点的路径，格式类似"/dir1/dir2/key"

	CreatedIndex  uint64		//记录创建当前节点时对应的CurrentIndex值
	ModifiedIndex uint64		//记录最后一次更新当前节点时对应的CurrentIndex值

	Parent *node `json:"-"` // should not encode this field! avoid circular dependency.		指向父节点的指针

	//当前节点的过期时间，如果该字段被设置为0，则表示当前节点是一个“永久节点”(即不会被过期删除)。
	ExpireTime time.Time
	Value      string           // for key-value pair		如果当前节点表示一个键值对，则该字段记录了对应的值
	//如果当前节点表示一个目录节点，则该字段记录了其子节点。node.IsDir()方法通过检查该字段判断当前节点是否为目录节点。
	Children   map[string]*node // for directory

	// A reference to the store this node is attached to.
	store *store				//记录当前节点关联的V2版本存储实例
}

// newKV creates a Key-Value pair
func newKV(store *store, nodePath string, value string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// newDir creates a directory
func newDir(store *store, nodePath string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ExpireTime:    expireTime,
		Children:      make(map[string]*node),
		store:         store,
	}
}

// IsHidden function checks if the node is a hidden node. A hidden node   该方法判断某个节点是否为“隐藏节点”
// will begin with '_'
// A hidden node will not be shown via get command under a directory
// For example if we have /foo/_hidden and /foo/notHidden, get "/foo"
// will only return /foo/notHidden
func (n *node) IsHidden() bool {
	_, name := path.Split(n.Path)		//按照最后一个"/"符号进行切分，获取节点所在的目录以及节点名称

	return name[0] == '_'				//检查当前节点是否以下划线开头
}

// IsPermanent function checks if the node is a permanent one.
func (n *node) IsPermanent() bool {
	// we use a uninitialized time.Time to indicate the node is a
	// permanent one.
	// the uninitialized time.Time should equal zero.
	return n.ExpireTime.IsZero()
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *node) IsDir() bool {
	return n.Children != nil
}

// Read function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
func (n *node) Read() (string, *etcdErr.Error) {
	if n.IsDir() {					//如果当前节点时目录节点，则抛出异常
		return "", etcdErr.NewError(etcdErr.EcodeNotFile, "", n.store.CurrentIndex)
	}

	return n.Value, nil			//返回value
}

// Write function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *node) Write(value string, index uint64) *etcdErr.Error {
	if n.IsDir() {					//如果当前节点是目录节点，则抛出异常
		return etcdErr.NewError(etcdErr.EcodeNotFile, "", n.store.CurrentIndex)
	}

	n.Value = value					//更新值
	n.ModifiedIndex = index			//记录更新时对应的CurrentIndex值

	return nil
}
//该方法提供了计算当前节点存活时间的功能
func (n *node) expirationAndTTL(clock clockwork.Clock) (*time.Time, int64) {
	if !n.IsPermanent() {								//检查当前节点是否为永久节点
		/* compute ttl as:
		   ceiling( (expireTime - timeNow) / nanosecondsPerSecond )
		   which ranges from 1..n
		   rather than as:
		   ( (expireTime - timeNow) / nanosecondsPerSecond ) + 1
		   which ranges 1..n+1
		*/
		ttlN := n.ExpireTime.Sub(clock.Now())			//节点过期时间减去当前时间，即为剩余的存活时间
		ttl := ttlN / time.Second						//以秒为单位，不足一秒则按一秒算
		if (ttlN % time.Second) > 0 {
			ttl++
		}
		t := n.ExpireTime.UTC()
		return &t, int64(ttl)							//返回当前节点的过期时间和剩余存活时间
	}
	return nil, 0										//永久节点对应的返回值
}
// 该方法主要负责获取当前节点(目录节点)的子节点。
// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
func (n *node) List() ([]*node, *etcdErr.Error) {
	if !n.IsDir() {										//检测当前节点是否为目录节点，如果当前节点不是目录节点，则抛出异常
		return nil, etcdErr.NewError(etcdErr.EcodeNotDir, "", n.store.CurrentIndex)
	}

	nodes := make([]*node, len(n.Children))

	i := 0
	for _, node := range n.Children {					//获取子节点
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// GetChild function returns the child node under the directory node.
// On success, it returns the file node
func (n *node) GetChild(name string) (*node, *etcdErr.Error) {
	if !n.IsDir() {
		return nil, etcdErr.NewError(etcdErr.EcodeNotDir, n.Path, n.store.CurrentIndex)
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil
}
// 该函数主要功能是在当前节点下添加子节点。
// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is an existing node with the same name under the directory, a "Already Exist"
// error will be returned
func (n *node) Add(child *node) *etcdErr.Error {
	if !n.IsDir() {							//检测当前节点是否为目录节点
		return etcdErr.NewError(etcdErr.EcodeNotDir, "", n.store.CurrentIndex)
	}

	_, name := path.Split(child.Path)		//按照"/"进行切分，得到待添加子节点对应的key

	if _, ok := n.Children[name]; ok {		//检测当前节点的Child字段中是否已存在待添加子节点
		return etcdErr.NewError(etcdErr.EcodeNodeExist, "", n.store.CurrentIndex)
	}

	n.Children[name] = child				//添加子节点

	return nil
}
// 该方法负责将当前节点从其父节点中删除，其中会根据参数决定是否递归删除当前节点的子节点；
// Remove function remove the node.
func (n *node) Remove(dir, recursive bool, callback func(path string)) *etcdErr.Error {
	if !n.IsDir() { // key-value pair		当前节点为KV节点
		_, name := path.Split(n.Path)		//获取当前节点对应的Key

		// find its parent and remove the node from the map		查找当前节点的父节点，并将当前节点从父节点的Child字段中删除
		if n.Parent != nil && n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
		}

		if callback != nil {				//回调callback函数
			callback(n.Path)
		}

		if !n.IsPermanent() {				//当前节点为非永久节点，则将其从store.ttlKeyHeap中删除，store.ttlKeyHeap中的节点时按照过期时间排序的
			n.store.ttlKeyHeap.remove(n)
		}

		return nil
	}
	//下面是针对目录节点的删除操作。只有dir参数设置为true，才能删除该目录节点
	if !dir {
		// cannot delete a directory without dir set to true
		return etcdErr.NewError(etcdErr.EcodeNotFile, n.Path, n.store.CurrentIndex)
	}
	//如果当前目录节点还存在子节点，且不能进行递归删除(recursive参数设置为false)，则不能删除该目录节点
	if len(n.Children) != 0 && !recursive {
		// cannot delete a directory if it is not empty and the operation
		// is not recursive
		return etcdErr.NewError(etcdErr.EcodeDirNotEmpty, n.Path, n.store.CurrentIndex)
	}
	//递归删除当前目录节点的全部子节点
	for _, child := range n.Children { // delete all children
		child.Remove(true, true, callback)
	}

	// delete self		下面是删除的当前节点本身，该代码片段与前面KV节点的删除过程相同
	_, name := path.Split(n.Path)
	if n.Parent != nil && n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}
	}

	return nil
}
//该方法会将当前节点和子节点(根据参数决定是否递归处理子节点以及子节点是否排序)转换成NodeExtern实例返回。
func (n *node) Repr(recursive, sorted bool, clock clockwork.Clock) *NodeExtern {
	if n.IsDir() {												//下面是针对目录节点的处理
		node := &NodeExtern{									//NodeExtern是node的外部表示形式
			Key:           n.Path,
			Dir:           true,
			ModifiedIndex: n.ModifiedIndex,
			CreatedIndex:  n.CreatedIndex,
		}
		node.Expiration, node.TTL = n.expirationAndTTL(clock)	//计算当前节点的过期时间和TTL(即当前节点还能存活多长时间)

		if !recursive {											//recursive参数表示是否递归获取子节点
			return node
		}

		children, _ := n.List()									//获取子节点
		node.Nodes = make(NodeExterns, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		for _, child := range children {						//遍历子节点

			if child.IsHidden() { // get will not list hidden node   	忽略隐藏节点
				continue
			}
			//调用子节点的Repr()方法，创建对应的NodeExtern实例
			node.Nodes[i] = child.Repr(recursive, sorted, clock)

			i++
		}

		// eliminate hidden nodes
		node.Nodes = node.Nodes[:i]								//这里简单进行压缩，释放隐藏节点导致的空间浪费
		if sorted {												//根据sorted参数，决定是否对子节点进行排序
			sort.Sort(node.Nodes)
		}

		return node
	}
	// 下面是针对KV节点的处理
	// since n.Value could be changed later, so we need to copy the value out
	value := n.Value											//复制当前节点的Value值
	node := &NodeExtern{										//创建当前节点对应的NodeExtern实例，并初始化相应字段
		Key:           n.Path,
		Value:         &value,
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
	node.Expiration, node.TTL = n.expirationAndTTL(clock)		//计算当前节点的过期时间和TTL
	return node
}
//该方法会更新指定节点的过期时间，另外还会更新该节点在TTLKeyHeap中的排序
func (n *node) UpdateTTL(expireTime time.Time) {
	if !n.IsPermanent() {										//对非永久节点的处理
		if expireTime.IsZero() {
			// from ttl to permanent
			n.ExpireTime = expireTime							//更新当前节点的过期时间
			// remove from ttl heap
			n.store.ttlKeyHeap.remove(n)						//更新当前节点在TTLKeyHeap中排序
			return
		}
		// 下面是将永久节点变成非永久节点
		// update ttl
		n.ExpireTime = expireTime
		// update ttl heap
		n.store.ttlKeyHeap.update(n)							//将当前节点添加到TTLKeyHeap中
		return
	}

	if expireTime.IsZero() {
		return
	}

	// from permanent to ttl
	n.ExpireTime = expireTime
	// push into ttl heap
	n.store.ttlKeyHeap.push(n)
}

// Compare function compares node index and value with provided ones.
// second result value explains result and equals to one of Compare.. constants
func (n *node) Compare(prevValue string, prevIndex uint64) (ok bool, which int) {
	indexMatch := (prevIndex == 0 || n.ModifiedIndex == prevIndex)
	valueMatch := (prevValue == "" || n.Value == prevValue)
	ok = valueMatch && indexMatch
	switch {
	case valueMatch && indexMatch:
		which = CompareMatch
	case indexMatch && !valueMatch:
		which = CompareValueNotMatch
	case valueMatch && !indexMatch:
		which = CompareIndexNotMatch
	default:
		which = CompareNotMatch
	}
	return ok, which
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *node) Clone() *node {
	if !n.IsDir() {
		newkv := newKV(n.store, n.Path, n.Value, n.CreatedIndex, n.Parent, n.ExpireTime)
		newkv.ModifiedIndex = n.ModifiedIndex
		return newkv
	}

	clone := newDir(n.store, n.Path, n.CreatedIndex, n.Parent, n.ExpireTime)
	clone.ModifiedIndex = n.ModifiedIndex

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// recoverAndclean function help to do recovery.
// Two things need to be done: 1. recovery structure; 2. delete expired nodes
//
// If the node is a directory, it will help recover children's parent pointer and recursively
// call this function on its children.
// We check the expire last since we need to recover the whole structure first and add all the
// notifications into the event history.
func (n *node) recoverAndclean() {
	if n.IsDir() {
		for _, child := range n.Children {
			child.Parent = n
			child.store = n.store
			child.recoverAndclean()
		}
	}

	if !n.ExpireTime.IsZero() {
		n.store.ttlKeyHeap.push(n)
	}
}
