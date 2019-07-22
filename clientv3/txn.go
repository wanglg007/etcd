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

package clientv3

import (
	"context"
	"sync"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"

	"google.golang.org/grpc"
)

// Txn is the interface that wraps mini-transactions.
//
//	 Txn(context.TODO()).If(
//	  Compare(Value(k1), ">", v1),
//	  Compare(Version(k1), "=", 2)
//	 ).Then(
//	  OpPut(k2,v2), OpPut(k3,v3)
//	 ).Else(
//	  OpPut(k4,v4), OpPut(k5,v5)
//	 ).Commit()
//
type Txn interface {
	// If takes a list of comparison. If all comparisons passed in succeed,		添加Compare条件
	// the operations passed into Then() will be executed. Or the operations
	// passed into Else() will be executed.
	If(cs ...Cmp) Txn

	// Then takes a list of operations. The Ops list will be executed, if the	如果Compare条件全部成立，则执行Then()方法中添加的全部操作
	// comparisons passed in If() succeed.
	Then(ops ...Op) Txn

	// Else takes a list of operations. The Ops list will be executed, if the	如果Compare条件不成立，则执行Else()方法中添加全部操作
	// comparisons passed in If() fail.
	Else(ops ...Op) Txn

	// Commit tries to commit the transaction.									提交Txn
	Commit() (*TxnResponse, error)
}

type txn struct {
	kv  *kv							//关联的kv实例
	ctx context.Context

	mu    sync.Mutex
	cif   bool						//是否设置Compare条件，设置一次则该字段就会被设置为true
	cthen bool						//是否向上面的sus集合中添加了操作
	celse bool						//是否向上面的fas集合中添加了操作

	isWrite bool					//如果当前txn实例中记录的操作全部是读操作，则该字段为false，否则为true

	cmps []*pb.Compare				//用于记录Compare条件的集合

	sus []*pb.RequestOp				//全部Compare条件通过之后执行的操作列表
	fas []*pb.RequestOp				//任一Compare条件检测失败之后执行的操作列表

	callOpts []grpc.CallOption
}

func (txn *txn) If(cs ...Cmp) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.cif {
		panic("cannot call If twice!")
	}

	if txn.cthen {
		panic("cannot call If after Then!")
	}

	if txn.celse {
		panic("cannot call If after Else!")
	}

	txn.cif = true

	for i := range cs {
		txn.cmps = append(txn.cmps, (*pb.Compare)(&cs[i]))
	}

	return txn
}

func (txn *txn) Then(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.cthen {
		panic("cannot call Then twice!")
	}
	if txn.celse {
		panic("cannot call Then after Else!")
	}

	txn.cthen = true

	for _, op := range ops {
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.sus = append(txn.sus, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Else(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.celse {
		panic("cannot call Else twice!")
	}

	txn.celse = true

	for _, op := range ops {
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.fas = append(txn.fas, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Commit() (*TxnResponse, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	r := &pb.TxnRequest{Compare: txn.cmps, Success: txn.sus, Failure: txn.fas}		//创建TxnRequest实例

	var resp *pb.TxnResponse
	var err error
	resp, err = txn.kv.remote.Txn(txn.ctx, r, txn.callOpts...)						//调用KVClient.Txn()方法与服务端交互
	if err != nil {
		return nil, toErr(txn.ctx, err)
	}
	return (*TxnResponse)(resp), nil
}
