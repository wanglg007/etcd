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

package main

import (
	"errors"
	"net"
	"time"
)

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener				//内嵌net.TCPListener
	stopc <-chan struct{}			//用来监听是否应该关闭当前的Server，也就是raftNode中的httpstopc通道
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {						//启动单独的goroutine来接收新连接
		tc, err := ln.AcceptTCP()		//异常处理
		if err != nil {
			errc <- err
			return
		}
		connc <- tc						//将接收到新连接传递到connc通道中
	}()
	select {
	case <-ln.stopc:					//从stopc通道中接收到通知时，会抛出异常
		return nil, errors.New("server stopped")
	case err := <-errc:					//如果在接收新连接时出现异常，则抛出异常
		return nil, err
	case tc := <-connc:					//接收到新建连接，设置KeepAlive等连接属性，并返回
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
