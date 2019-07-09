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

package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"

	"github.com/coreos/pkg/capnslog"
)

const (
	//该类型日志记录的Data字段中保存一些元数据，在每个WAL文件的开头，都会记录一条metadataType类型的日志记录
	metadataType int64 = iota + 1
	//该类型日志记录的Data字段中保存Entry记录，即客户端发送给服务端处理的数据
	entryType
	//该类型日志记录的Data字段中保存当前集群的状态信息(即HardState)，在每次批量写入entryType类型日志记录之前，都会先写入一条stateType类型的日志记录
	stateType
	//该类型的日志记录主要用于数据校验
	crcType
	//该类型的日志记录中保存了快照数据的相关信息
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "wal")

	ErrMetadataConflict = errors.New("wal: conflicting metadata found")
	ErrFileNotFound     = errors.New("wal: file not found")
	ErrCRCMismatch      = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound = errors.New("wal: snapshot not found")
	crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	dir string // the living directory of the underlay files				存放WAL日志文件的目录路径

	// dirFile is a fd for the wal directory for syncing on Rename
	dirFile *os.File														//根据dir路径创建的File实例

	metadata []byte           // metadata recorded at the head of each WAL	在每个WAL日志文件的头部，都会写入metadata元数据
	//WAL日志记录的追加是批量的，在每次批量写入entryType类型的日志之后，都会再追加一条stateType类型的日志记录，在HardState中记录了当前的Term、当前节点的投票结果和已提交日志的位置
	state    raftpb.HardState // hardstate recorded at the head of WAL

	//每次读取WAL日志时，并不会每次都从头开始读取，而是通过start字段指定具体的起始位置
	start     walpb.Snapshot // snapshot to start reading
	decoder   *decoder       // decoder to decode records					负责在读取WAL日志文件时，将二进制数据反序列化成Record实例
	readClose func() error   // closer for decode reader

	mu      sync.Mutex														//读写WAL日志时需要加锁同步
	enti    uint64   // index of the last entry saved to the wal			WAL中最后一条Entry记录的索引值
	encoder *encoder // encoder to encode records							负责将写入WAL日志文件的Record实例进行序列化成二进制数据

	//当前WAL实例关联的所有WAL日志文件对应的句柄
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	fp    *filePipeline														//filePipeline实例负责创建新的临时文件
}
// 其步骤如下:(1)创建临时目录，并在临时目录中创建编号为"0-0"的WAL日志文件，WAL日志文件名由两部分组成，一部分是seq(单调递增)，另一部分是该日志文件中第一条日志记录的
// 索引值；(2)尝试为该WAL日志文件预分配磁盘空间；(3)向该WAL日志we年中写入一条crcType类型的日志记录、一条metadataType类型的日志记录及一条snapshotType类型的日志记录；
//(4)创建WAL实例关联的filePipeline实例；(5)将临时目录重命名为WAL.dir字段指定的名称；
// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll.
func Create(dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) {										//检测文件夹是否存在
		return nil, os.ErrExist
	}

	// keep temporary wal directory so WAL initialization appears atomic
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"			//得到临时目录的路径
	if fileutil.Exist(tmpdirpath) {							//清空临时目录中的文件
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {//创建临时文件夹
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0))//第一个WAL日志文件的路径(文件名为0-0)
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)		//创建临时文件，注意文件的模式和权限
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {	  //移动临时文件的offset到文件结尾处
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {	//对新建的临时文件进行空间预分配，默认值是64MB(SegmentSizeBytes)
		return nil, err
	}

	w := &WAL{												 //创建WAL实例
		dir:      dirpath,									 //存放WAL日志文件的目录的路径
		metadata: metadata,									 //元数据
	}
	w.encoder, err = newFileEncoder(f.File, 0)		 //创建写WAL日志文件的encoder
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f)							 //将WAL日志文件对应的LockedFile实例记录到locks字段中，表示当前WAL实例正在管理该日志文件
	if err = w.saveCrc(0); err != nil {			 //创建一条crcType类型的日志写入WAL日志文件
		return nil, err
	}
	//将元数据封装成一条metadataType类型的日志记录写入WAL日志文件
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {	 //创建一条空的snapshotType类型的日志记录写入临时文件
		return nil, err
	}

	if w, err = w.renameWal(tmpdirpath); err != nil {		 //将临时目录重命名，并创建WAL实例关联的filePipeline实例
		return nil, err
	}

	// directory was renamed; sync parent dir to persist rename  	临时目录重命名之后，需要将重命名操作刷新到磁盘上
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		return nil, perr
	}
	if perr = fileutil.Fsync(pdir); perr != nil {			 //同步磁盘的操作
		return nil, perr
	}
	if perr = pdir.Close(); err != nil {
		return nil, perr
	}

	return w, nil											 //返回WAL实例
}

func (w *WAL) renameWal(tmpdirpath string) (*WAL, error) {	 //该方法是重命名临时目录并创建关联的filePipeline实例
	if err := os.RemoveAll(w.dir); err != nil {					//清空wal文件夹
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	if err := os.Rename(tmpdirpath, w.dir); err != nil {		//重命名临时文件夹
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWalUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.dir, SegmentSizeBytes)				//创建WAL实例关联的filePipeline实例
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df												//WAL.dirFile字段记录了WAL日志目录对应的文件句柄
	return w, err
}

func (w *WAL) renameWalUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	plog.Infof("releasing file lock to rename %q to %q", tmpdirpath, w.dir)
	w.Close()
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}
	// reopen and relock
	newWAL, oerr := Open(w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(dirpath, snap, false)
}

func openAtIndex(dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	names, nameIndex, err := selectWALFiles(dirpath, snap)
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	w := &WAL{
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil
		if _, _, err := parseWalName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	}

	return w, nil
}

func selectWALFiles(dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWalNames(dirpath)
	if err != nil {
		return nil, -1, err
	}

	nameIndex, ok := searchIndex(names, snap.Index)
	if !ok || !isValidSeq(names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

func openWALFiles(dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(rcs...) }

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}
	decoder := w.decoder

	var match bool
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			if e.Index > w.start.Index {
				ents = append(ents[:e.Index-w.start.Index-1], e)
			}
			w.enti = e.Index
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}
		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	switch w.tail() {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match {
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil {
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(walDir string, snap walpb.Snapshot) error {
	var metadata []byte
	var err error
	var match bool

	rec := &walpb.Record{}

	names, nameIndex, err := selectWALFiles(walDir, snap)
	if err != nil {
		return err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(walDir, names, nameIndex, false)
	if err != nil {
		return err
	}

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
		default:
			return fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	if closer != nil {
		closer()
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}

	if !match {
		return ErrSnapshotNotFound
	}

	return nil
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}
	if err := w.tail().Truncate(off); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}

	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	w.locks = append(w.locks, newTail)
	prevCrc := w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}
	if err = w.saveState(&w.state); err != nil {
		return err
	}
	// atomically move temp wal file to wal file
	if err = w.sync(); err != nil {
		return err
	}

	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}

	// reopen newTail with its new path so calls to Name() match the wal filename format
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	plog.Infof("segmented wal file %v is created", fpath)
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	err := fileutil.Fdatasync(w.tail().File)

	duration := time.Since(start)
	if duration > warnSyncDuration {
		plog.Warningf("sync duration of %v, expected less than %v", duration, warnSyncDuration)
	}
	syncDurations.Observe(duration.Seconds())

	return err
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false

	for i, l := range w.locks {
		_, lockIndex, err := parseWalName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			plog.Errorf("failed to unlock during closing wal: %s", err)
		}
	}

	return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	if err := w.saveState(&st); err != nil {
		return err
	}

	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		if mustSync {
			return w.sync()
		}
		return nil
	}

	return w.cut()
}

func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWalName(filepath.Base(t.Name()))
	if err != nil {
		plog.Fatalf("bad wal name %s (%v)", t.Name(), err)
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
