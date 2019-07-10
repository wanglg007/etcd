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

// Package snap stores raft nodes' states with snapshots.
package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pioutil "github.com/coreos/etcd/pkg/ioutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap/snappb"

	"github.com/coreos/pkg/capnslog"
)

const (
	snapSuffix = ".snap"
)

var (
	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "snap")

	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	dir string
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}
//该方法的主要功能是将快照数据保存到快照文件中，其底层是通过调用save()方法实现的。
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()
	//创建快照文件名，快照文件的名称由三部分组成，分别是快照所涵盖的最后一条Entry记录的Term、Index和".snap"后缀
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	b := pbutil.MustMarshal(snapshot)														//将快照数据进行序列化
	crc := crc32.Update(0, crcTable, b)												//计算crc32校验码
	//将序列化后的数据和校验码封装成snappb.Snapshot实例，raftpb.Snapshot和snappb.Snapshot的区别，前者包含了SnapShot数据及一些元数据(例如，该快照数据所涵盖的最后
	//一条Entry记录的Term和Index)；后者则是在前者序列化之后的封装，其中还记录了相应的校验码等信息
	snap := snappb.Snapshot{Crc: crc, Data: b}
	d, err := snap.Marshal()																//将snappb.Snapshot实例序列化
	if err != nil {
		return err
	} else {
		marshallingDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	}

	err = pioutil.WriteAndSyncFile(filepath.Join(s.dir, fname), d, 0666)				//将snappb.Snapshot序列化后的数据写入文件，并同步刷新到磁盘
	if err == nil {
		saveDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	} else {
		err1 := os.Remove(filepath.Join(s.dir, fname))
		if err1 != nil {
			plog.Errorf("failed to remove broken snapshot file %s", filepath.Join(s.dir, fname))
		}
	}
	return err
}
//该方法会读取指定目录下的全部快照文件，并查找其中最近的可用快照文件，然后通过Snapshotter.loadSnap()方法加载其中的快照数据。
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	//获取全部快照文件的名称，其中会检测文件的后缀名，过滤不合法的文件。另外，返回的快照文件的名称是经过排序的
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		//按序读取快照文件，直到读取到第一个合法的快照文件，对于读取失败的快照文件则添加".broken"后缀，这样在下次调用snapNames()方法获取可用快照文件时，就会将其过滤掉
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)		//获取快照文件的绝对路径
	snap, err := Read(fpath)				//读取快照文件的内容
	if err != nil {							//如果读取失败，则将快照文件的后缀名称修改成".broken"
		renameBroken(fpath)
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(snapname string) (*raftpb.Snapshot, error) {
	b, err := ioutil.ReadFile(snapname)		//读取文件
	if err != nil {
		plog.Errorf("cannot read file %v: %v", snapname, err)
		return nil, err
	}

	if len(b) == 0 {
		plog.Errorf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {					//将读取到的数据进行反序列化
		plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		plog.Errorf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		plog.Errorf("corrupted snapshot file %v: crc mismatch", snapname)
		return nil, ErrCRCMismatch
	}

	var snap raftpb.Snapshot											//使用crc32校验码验证数据是否正确
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {			//反序列化snappb.Snapshot实例的Data字段，得到raftpb.Snapshot实例
		plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				plog.Warningf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}

func renameBroken(path string) {
	brokenPath := path + ".broken"
	if err := os.Rename(path, brokenPath); err != nil {
		plog.Warningf("cannot rename broken snapshot file %v to %v: %v", path, brokenPath, err)
	}
}
