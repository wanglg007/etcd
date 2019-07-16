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

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/coreos/pkg/capnslog"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc/backend")

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = time.Duration(30 * time.Second)
)
//Backend接口的主要功能是将底层存储与上层进行解耦，其中定义底层存储需要对上层提供的接口。
type Backend interface {
	//创建一个只读事务，这里的ReadTx接口是V3存储对只读事务的抽象
	ReadTx() ReadTx
	//创建一个批量事务，这里的BatchTx接口是对批量读写事务的抽象
	BatchTx() BatchTx

	Snapshot() Snapshot															//创建快照
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend.
	Size() int64																//获取当前已存储的总字节数
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	SizeInUse() int64
	Defrag() error																//碎片整理
	ForceCommit()																//提交批量读写事务
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}
//该结构体是V3版本存储提供的Backend接口的默认实现，其底层存储使用的就是BoltDB
type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes in the backend
	size int64												//当前backend实例已存储的总字节数

	// sizeInUse is the number of bytes actually used in the backend
	sizeInUse int64

	// commits counts number of commits since start
	commits int64											//从启动到目前为止，已经提交的事务数

	mu sync.RWMutex
	db *bolt.DB												//底层的BoltDB存储

	batchInterval time.Duration								//两次批量读写事务提交的最大时间差
	batchLimit    int										//指定一次批量事务中最大的操作数，当超过阈值时，当前的批量事务会自动提交
	batchTx       *batchTxBuffered							//批量读写事务，batchTxBuffered是在batchTx的基础上添加了缓存功能

	readTx *readTx											//只读事务

	stopc chan struct{}
	donec chan struct{}
}

type BackendConfig struct {
	// Path is the file path to the backend file.						BoltDB数据库文件的路径
	Path string
	// BatchInterval is the maximum time before flushing the BatchTx.	提交两次批量事务的最大时间差，用来初始化backend实例中的batchInterval字段，默认值是100ms
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.		指定每个批量读写事务能包含的最多的操作个数，当超过这个阈值之后，当前批量读写事务自动提交
	BatchLimit int
	// MmapSize is the number of bytes to mmap for the backend.
	// BoltDB使用mmap技术对数据库文件建映射，该字段用来设置mmap中使用的内存大小，该字段会在创建BoltDB实例时使用
	MmapSize uint64
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	bopts := &bolt.Options{}											//初始化BoltDB时的参数
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = bcfg.mmapSize()								//mmap使用的内存大小

	db, err := bolt.Open(bcfg.Path, 0600, bopts)						//创建bolt.DB实例
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", bcfg.Path, err)
	}

	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	b := &backend{														//创建backend实例，并初始化其中各个字段
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,
		//创建readTx实例并初始化backend.readTx字段
		readTx: &readTx{
			buf: txReadBuffer{
				txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			},
			buckets: make(map[string]*bolt.Bucket),
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	b.batchTx = newBatchTxBuffered(b)			//创建batchTxBuffered实例并初始化backend.batchTx字段
	go b.run()									//启动一个单独的goroutine，其中会定时提交当前的批量读写事务，并开启新的批量读写事务
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }
// 该方法会提交当前的读写事务并立即开启新的读写事务
// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}
//该方法的主要功能是用当前的BoltDB中的数据创建相应的快照，其中使用前面的Tx.WriteTo()方法备份整个BoltDB数据库中的数据。
func (b *backend) Snapshot() Snapshot {
	b.batchTx.Commit()					//提交当前的读写事务，主要是为了提交缓冲区中的操作

	b.mu.RLock()						//加锁和解锁操作
	defer b.mu.RUnlock()
	tx, err := b.db.Begin(false)		//开启一个只读事务
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	dbBytes := tx.Size()				//获取整个BoltDB中保存的数据
	go func() {						//启动一个单独的goroutine，用来检测快照数据是否已经发送完成
		defer close(donec)
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		var sendRateBytes int64 = 100 * 1024 * 1014			//这里假设发送快照的最小速度为100MB/s
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))		//创建定时器
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:											//超时未发送完快照数据，则会输出警告日志
				plog.Warningf("snapshotting is taking more than %v seconds to finish transferring %v MB [started at %v]", time.Since(start).Seconds(), float64(dbBytes)/float64(1024*1014), start)
			case <-stopc:												//发送快照数据结束
				snapshotDurations.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	return &snapshot{tx, stopc, donec}				//创建快照实例
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}
//该方法会按照batchInterval指定的时间间隔，定时提交批量读写数据，在提交之后会立即开启一个新的批量读写事务。
func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval)							//创建定时器
	defer t.Stop()
	for {
		select {												//阻塞等待上述定时器到期
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		b.batchTx.Commit()										//提交当前的批量读写事务，并开启一个新的批量读写事务
		t.Reset(b.batchInterval)								//重置定时器
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}
//该方法的主要功能是整理当前BoltDB实例中的碎片，其实就是提高其中Bucket的填充率。整理碎片实际上就是创建新的BoltDB数据库文件并将旧数据库文件中的数据写入新数据库
//文件中。因为在写入新数据库文件时是顺序写入的，所以会提高填充比例，从而达到整理碎片的目的。需要注意的是，在整理碎片的过程中，需要持有readTx、batchTx和backend
//的锁。
func (b *backend) Defrag() error {
	return b.defrag()								//调用defrag()方法完成具体的碎片整理过程
}
//下面是backend.defrag()方法的具体实现，其中主要完成了新老数据库文件的切换。
func (b *backend) defrag() error {
	now := time.Now()
	// 加锁，这里会获取readTx、batchTx和backend中的三把锁
	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.mu.Lock()
	defer b.readTx.mu.Unlock()

	b.batchTx.unsafeCommit(true)									//提交当前的批量读写事务，注意参数，此次提交后不会立即打开新的批量读写事务
	b.batchTx.tx = nil

	tmpdb, err := bolt.Open(b.db.Path()+".tmp", 0600, boltOpenOptions)	//创建新的bolt.DB实例，对应的数据库文件是个临时文件
	if err != nil {
		return err
	}
	//进行碎片整理，其底层是创建一个新的BoltDB数据库文件并将当前数据库中的全部数据写入到其中，在写入过程中，会将新Bucket的填充比例设置成90%，从而达到
	//碎片整理的效果
	err = defragdb(b.db, tmpdb, defragLimit)

	if err != nil {
		tmpdb.Close()
		os.RemoveAll(tmpdb.Path())
		return err
	}

	dbp := b.db.Path()					//获取旧数据库文件的路径
	tdbp := tmpdb.Path()				//获取新数据库文件的路径

	err = b.db.Close()					//关闭旧的bolt.DB实例
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	err = tmpdb.Close()					//关闭新的bolt.DB实例
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	err = os.Rename(tdbp, dbp)			//重命名新数据库文件，覆盖旧数据库文件
	if err != nil {
		plog.Fatalf("cannot rename database (%s)", err)
	}

	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)				//重新创建bolt.DB实例，此时使用的数据库文件是整理之后的新数据库文件
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", dbp, err)
	}
	b.batchTx.tx, err = b.db.Begin(true)							//开启新的批量读写事务以及只读事务
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}

	b.readTx.reset()
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.db
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragDurations.Observe(took.Seconds())

	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)				//在新数据库实例上开启一个读写事务
	if err != nil {
		return err
	}

	// open a tx on old db for read
	tx, err := odb.Begin(false)					//在旧数据库实例上开启一个只读事务
	if err != nil {
		return err
	}
	defer tx.Rollback()							//方法结束时关闭该只读事务

	c := tx.Cursor()							//获取旧实例上的Cursor，用于遍历其中的所有Bucket

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {		//读取旧数据库实例中的所有Bucket，并在新数据库实例上创建对应的Bucket
		b := tx.Bucket(next)					//获取指定的Bucket实例
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		//为提供利用率，将填充比例设置为90%，因为下面会从读取旧Bucket中全部的键值对，并填充到新Bucket中，这个过程是顺序写入的。
		tmpb.FillPercent = 0.9 // for seq write in for each

		b.ForEach(func(k, v []byte) error {				//遍历旧Bucket中的全部键值对
			count++
			if count > limit {							//当读取的键值对数量超过阈值，则提交当前读写事务(新数据库)
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)			//重新开启一个读写事务，继续后面的写入操作
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for seq write in for each					设置填充比例

				count = 0
			}
			return tmpb.Put(k, v)						//将读取到的键值对写入新数据库文件中
		})
	}

	return tmptx.Commit()								//最后提交读写事务(新数据库)
}
//开启新的只读事务或读写事务都是通过该方法实现的。该方法除了会开启事务，还会更新backend中的相关字段。
func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock()
	tx := b.unsafeBegin(write)							//开启事务
	b.mu.RUnlock()

	size := tx.Size()
	db := tx.DB()
	atomic.StoreInt64(&b.size, size)					//更新backend.size字段，记录了当前数据库的大小
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write)						//调用了BoltDB的API开启事务
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}
	return tx
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		plog.Fatal(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
