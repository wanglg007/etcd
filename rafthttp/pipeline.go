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

package rafthttp

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")
//该结构体负责传输快照数据
type pipeline struct {
	peerID types.ID							//对应节点的ID

	tr     *Transport						//关联的rafthttp.Transport实例
	picker *urlPicker
	status *peerStatus
	raft   Raft								//底层的Raft实例
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	msgc chan raftpb.Message				//pipeline实例从该通道中获取待发送的消息
	// wait for the handling routines
	wg    sync.WaitGroup					//负责同步多个goroutine结束
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan raftpb.Message, pipelineBufSize)				//注意缓存，默认是64，主要是为了防止瞬间网络延迟造成消息丢失
	p.wg.Add(connPerPipeline)										//初始化sync.WaitGroup
	for i := 0; i < connPerPipeline; i++ {							//启动用于发送消息的goroutine(默认是4个)
		go p.handle()
	}
	plog.Infof("started HTTP pipelining with peer %s", p.peerID)
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()
	plog.Infof("stopped HTTP pipelining with peer %s", p.peerID)
}
//该方法会从msgc通道中读取待发送的Message消息，然后调用pipeline.post()方法将其发送出去，发送结束之后会调用底层Raft接口的相应方法报告发送结果。
func (p *pipeline) handle() {
	defer p.wg.Done()								//handle()方法执行完成，也就是当前这个goroutine结束

	for {
		select {
		case m := <-p.msgc:							//获取待发送的MsgSnap类型的Message
			start := time.Now()
			err := p.post(pbutil.MustMarshal(&m))	//将消息序列化，然后创建HTTP请求并发送出去
			end := time.Now()

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				p.raft.ReportUnreachable(m.To)		//向底层的Raft状态机报告失败信息
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}

			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			if isMsgSnap(m) {						//向底层的Raft状态机报告发送成功的信息
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,    该方法是真正完成消息发送的地方，其中会启动一个后台goroutine监听控制发送过程及
// error on any failure.                                                    获取发送结果。
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick()													//获取对端暴露的URL地址
	//创建HTTP POST请求
	req := createPostRequest(u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	done := make(chan struct{}, 1)											//主要用于通知下面的goroutine请求是否已经发送完成
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() {															//该goroutine主要用于监听请求是否需要取消
		select {
		case <-done:
		case <-p.stopc:														//如果在请求的发送过程中，pipeline被关闭，则取消该请求
			waitSchedule()
			cancel()														//取消请求
		}
	}()

	resp, err := p.tr.pipelineRt.RoundTrip(req)								//发送上述HTTP POST请求，并获取到对应的响应
	done <- struct{}{}														//通知上述goroutine，请求已经发送完毕
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	b, err := ioutil.ReadAll(resp.Body)										//读取HttpResponse.Body内容
	if err != nil {
		p.picker.unreachable(u)												//出现异常时，则将该URL标致为不可用，再尝试其他URL地址
		return err
	}
	resp.Body.Close()

	err = checkPostResponse(resp, b, req, p.peerID)							//检测响应的内容
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
