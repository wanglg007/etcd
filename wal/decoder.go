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
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"github.com/coreos/etcd/pkg/crc"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"
)

const minSectorSize = 512

// frameSizeBytes is frame size in bytes, including record size and padding size.
const frameSizeBytes = 8

type decoder struct {
	mu  sync.Mutex				//在decoder开始读取日志文件时，需要加锁同步
	brs []*bufio.Reader			//该decoder实例通过该字段中记录的Reader实例读取相应的日志文件

	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64			//读取日志记录的指针
	crc          hash.Hash32	//校验码
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {							//检测brs字段长度，决定是否还有日志文件需要读取
		return io.EOF
	}

	l, err := readInt64(d.brs[0])					//读取第一个日志文件中的第一个日志记录的长度
	if err == io.EOF || (err == nil && l == 0) {	//是否读到文件尾，或是读取到了预分配的部分，这都表示读取操作结束
		// hit end of file or preallocated space
		d.brs = d.brs[1:]							//更新brs字段，将其中第一个日志文件对应的Reader清除掉
		if len(d.brs) == 0 {						//如果后面没有其他日志文件可读则返回EOF异常，表示读取正常结束
			return io.EOF
		}
		d.lastValidOff = 0							//若后续还有其他日志文件待读取，则需要切换文件，这里会重置lastValidOff
		return d.decodeRecord(rec)					//递归调用decodeRecord方法
	}
	if err != nil {
		return err
	}

	recBytes, padBytes := decodeFrameSize(l)		//计算当前日志记录的实际长度及填充数据的长度，并创建相应的data切片

	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {				//从日志文件中读取指定长度的字节数。如果读取不到指定的字节数，则会返回EOF异常，此时认为读取到了
		// ReadFull returns io.EOF only if no bytes were read           写入一半的日志记录，需要返回ErrUnexpectedEOF异常
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := rec.Unmarshal(data[:recBytes]); err != nil {				//将0~recBytes反序列化成Record
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// skip crc checking if the record type is crcType
	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += frameSizeBytes + recBytes + padBytes				//将lastValidOff后移，准备读取下一条日志记录
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
