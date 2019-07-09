// Copyright 2016 The etcd Authors
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
	"fmt"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/pkg/fileutil"
)

// filePipeline pipelines allocating disk space   负责预创建日志文件并为日志文件预分配空间
type filePipeline struct {
	// dir to put files						存放临时文件的目录
	dir string
	// size of files to make, in bytes		创建临时文件时预分配空间的大小，默认是64MB(由wal.SegmentSizeBytes指定，该值也是每个日志文件的大小)
	size int64
	// count number of files generated		当前filePiopeline实例创建的临时文件数
	count int

	filec chan *fileutil.LockedFile			//新建的临时文件句柄会通过filec通道返回给WAL实例使用
	errc  chan error						//当创建临时文件出现异常时，则将异常传递到errc通道中
	donec chan struct{}					//当filePipeline.Close()被调用时会关闭donec通道，从而通知filePipeline实例删除最后一次创建的临时文件
}
//该方法除了创建filePipeline实例，还会启动一个后台goroutine来执行filePipeline.run()方法，该后台goroutine中会创建新的临时文件并将其句柄传递到filec通道中。
func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling   在WAL切换日志文件时会调用filePipeline.Open方法，从filec通道中获取之前创建好的临时文件
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:			//从filec通道中获取已经创建好的临时文件，并返回
	case err = <-fp.errc:			//如果创建临时文件时的异常，则通过errc通道中获取，并返回
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))			//为了防止与前一个创建的临时文件重名，新建临时文件的编号是0或是1
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {	//创建临时文件，注意文件的模式和权限
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {		//尝试预分配空间，如果当前系统不支持预分配文件空间，则并不会报错
		plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		f.Close()																		//如果出现异常，则会关闭donec通道
		return nil, err
	}
	fp.count++																			//递增创建的文件数量
	return f, nil																	//返回创建的临时文件
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()			//调用alloc方法创建临时文件
		if err != nil {					//如果创建临时文件失败，则将异常传递到errc通道中
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:				//将上面创建的临时文件句柄传递到filec通道中
		case <-fp.donec:				//关闭时触发，删除最后一次创建的临时文件
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
