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

package raft

import (
	"fmt"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type Status struct {
	ID uint64

	pb.HardState
	SoftState

	Applied  uint64
	Progress map[uint64]Progress

	LeadTransferee uint64
}

// getStatus gets a copy of the current raft status.
func getStatus(r *raft) Status {
	s := Status{												//创建Status实例，其ID字段为当前节点的id
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}

	s.HardState = r.hardState()									//从底层的raft实例中获取HardState实例
	s.SoftState = *r.softState()								//从底层的raft实例中获取SoftState实例

	s.Applied = r.raftLog.applied								//获取底层的raftLog中记录已应用的位置

	if s.RaftState == StateLeader {								//如果当前节点是Leader状态，则将集群中每个节点对应的Progress实例封装到Status实例中
		s.Progress = make(map[uint64]Progress)
		for id, p := range r.prs {
			s.Progress[id] = *p
		}

		for id, p := range r.learnerPrs {
			s.Progress[id] = *p
		}
	}

	return s
}

// MarshalJSON translates the raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
