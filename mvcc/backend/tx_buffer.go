// Copyright 2017 The etcd Authors
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

package backend

import (
	"bytes"
	"sort"
)

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[string]*bucketBuffer
}
//该方法清空buckets字段中的全部内容
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets {				//遍历buckets
		if v.used == 0 {							//删除未使用的bucketBuffer
			// demote
			delete(txb.buckets, k)
		}
		v.used = 0									//清空使用过的bucketBuffer
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	seq bool		//用于标记写入当前txWriteBuffer的键值对是否为顺序的
}
//该方法调用putSeq()方法，同时将seq字段设置为false，表示非顺序写入。
func (txw *txWriteBuffer) put(bucket, k, v []byte) {
	txw.seq = false
	txw.putSeq(bucket, k, v)
}
//该方法完成了向指定bucketBuffer添加键值对的功能
func (txw *txWriteBuffer) putSeq(bucket, k, v []byte) {
	b, ok := txw.buckets[string(bucket)]				//获取指定的bucketBuffer
	if !ok {											//如果未查找到，则创建对应的bucketBuffer实例并保存到buckets中
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}
	b.add(k, v)											//通过bucketBuffer.add()方法添加键值对
}
//该方法会将当前txWriteBuffer中键值对合并到指定的txReadBuffer实例中，这样可以达到更新只读事务缓存的效果。
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	for k, wb := range txw.buckets {					//遍历所有的bucketBuffer
		rb, ok := txr.buckets[k]						//从传入的bucketBuffer中查找指定的bucketBuffer
		//如果txReadBuffer中不存在对应的bucketBuffer，则直接使用txWriteBuffer中缓存的bucketBuffer实例
		if !ok {
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		if !txw.seq && wb.used > 1 {
			// assume no duplicate keys
			sort.Sort(wb)				//如果当前txWriteBuffer中的键值对是非顺序写入的，则需要先进行排序
		}
		rb.merge(wb)					//通过bucketBuffer.merge()方法，合并两个bucketBuffer实例并去重
	}
	txw.reset()							//清空txWriteBuffer
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct{ txBuffer }

func (txr *txReadBuffer) Range(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if b := txr.buckets[string(bucketName)]; b != nil {				//查询指定的txBuffer实例
		return b.Range(key, endKey, limit)							//查询txBuffer实例
	}
	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	if b := txr.buckets[string(bucketName)]; b != nil {				//查找指定的txBuffer实例
		return b.ForEach(visitor)									//调用txBuffer.ForEach()方法完成遍历
	}
	return nil
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	//每个元素都表示一个键值对，kv.key和kv.value都是[]byte类型。在初始化时，该切片的默认大小是512
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.  该字段记录buf中目前使用的下标位置
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, 512), used: 0}
}

func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }				//定义key的比较方式
	idx := sort.Search(bb.used, f)															//查询0~used之间是否有指定的key
	if idx < 0 {
		return nil, nil
	}
	if len(endKey) == 0 {																	//没有指定endKey，则只返回key对应的键值对
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {					//如果指定了endKey，则检测endKey的合法性
		return nil, nil
	}
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {		//从前面查找到的idx位置开始遍历，直到遍历到endKey或是遍历的键值对个数达到limit上限为止
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	return keys, vals													//返回全部符合条件的键值对
}
//该烦恼官方提供了遍历当前bucketBuffer实例缓存的所有键值对的功能。其中会调用传入的visitor()函数处理每个键值对。
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	for i := 0; i < bb.used; i++ {										//遍历used之前的所有元素
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {	//调用visitor()函数处理键值对
			return err
		}
	}
	return nil
}
//该方法提供了添加键值对缓存的功能，当buf的空间被用尽时，会进行扩容。
func (bb *bucketBuffer) add(k, v []byte) {
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v						//添加键值对
	bb.used++															//递增used
	if bb.used == len(bb.buf) {											//当buf空间被用尽时，对其进行扩容
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}
//该方法主要负责将传入的bbsrc(bucketBuffer实例)与当前的bucketBuffer进行合并，之后会对合并结果进行排序和去重。
// merge merges data from bb into bbsrc.
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	for i := 0; i < bbsrc.used; i++ {					//将bbsrc中的键值对添加到当前bucketBuffer中
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	if bb.used == bbsrc.used {							//复制之前，如果当前bucketBuffer是空的，则复制完键值对之后直接返回
		return
	}
	//复制之前，如果当前bucketBuffer不是空的，则需要判断复制之后，是否需要进行排序
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}
	//如果需要排序，则调用sort.Stable()函数对bucketBuffer进行排序。注意，Stable()函数进行的是稳定排序，即相等键值对的相对位置在排序之后不会改变
	sort.Stable(bb)

	// remove duplicates, using only newest update
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ {							//清除重复的key，使用key的最新值
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		bb.buf[widx] = bb.buf[ridx]									//新添加的键值对覆盖原有的键值对
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }
