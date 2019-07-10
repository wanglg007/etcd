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

package snap

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
)

var ErrNoDBSnapshot = errors.New("snap: snapshot file doesn't exist")

// SaveDBFrom saves snapshot of the database from the given reader. It
// guarantees the save operation is atomic.
func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	start := time.Now()

	f, err := ioutil.TempFile(s.dir, "tmp")				//创建临时文件
	if err != nil {
		return 0, err
	}
	var n int64
	n, err = io.Copy(f, r)											//将快照数据写入临时文件中
	if err == nil {
		fsyncStart := time.Now()
		err = fileutil.Fsync(f)										//将临时文件的改动刷新到磁盘
		snapDBFsyncSec.Observe(time.Since(fsyncStart).Seconds())
	}
	f.Close()														//关闭临时文件
	if err != nil {
		os.Remove(f.Name())											//如果上述过程中出现异常，则删除临时文件
		return n, err
	}
	fn := s.dbFilePath(id)											//获取指定的"snap.db"文件，如果存在则将其删除
	if fileutil.Exist(fn) {
		os.Remove(f.Name())
		return n, nil
	}
	err = os.Rename(f.Name(), fn)									//重命名临时文件
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}

	plog.Infof("saved database snapshot to disk [total bytes: %d]", n)

	snapDBSaveSec.Observe(time.Since(start).Seconds())
	return n, nil
}

// DBFilePath returns the file path for the snapshot of the database with   该方法是在dbFilePath()方法之上实现的，用于查找指定的快照db文件
// given id. If the snapshot does not exist, it returns error.
func (s *Snapshotter) DBFilePath(id uint64) (string, error) {
	//尝试读取快照目录，主要是检测快照文件是否存在
	if _, err := fileutil.ReadDir(s.dir); err != nil {
		return "", err
	}
	//获取指定的快照db文件的绝对路径
	if fn := s.dbFilePath(id); fileutil.Exist(fn) {
		return fn, nil
	}
	return "", ErrNoDBSnapshot
}

func (s *Snapshotter) dbFilePath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%016x.snap.db", id))
}
