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

package raft

import pb "etcd-with-comments/raft/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	//记录了对应的MsgIndex请求
	req pb.Message
	//该MsgReadIndex请求到达时，对应的已提交位置
	index uint64
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	//记录了该MsgReadIndex相关的MsgHeartbeatResp响应信息
	acks map[uint64]bool
}

//作用是批量处理只读请求
type readOnly struct {
	//当前只读请求的处理模式
	option ReadOnlyOption
	//在etcd服务端收到MsgReadIndex消息时，会为其创建一个消息id（该id是唯一的）
	//并作为MsgReadIndex消息的第一条Entry记录
	//在pendingReadIndex中记录了消息id与对应请求的readIndexStatus实例的映射
	pendingReadIndex map[string]*readIndexStatus
	//记录了MsgReadIndex请求对应的消息id
	readIndexQueue []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	//在ReadIndex消息的第一个记录中，记录了消息id
	s := string(m.Entries[0].Data)
	//检查是否存在id相同的MsgReadIndex,如果存在，则不再记录该MsgReadIndex请求
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	//创建MsgReadIndex对应的readIndexStatus实例，并记录到pendingReadIndex中
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)}
	//记录消息id
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	//获取消息id对应的readIndexStatus实例，省略异常处理的代码
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	//表示MsgHeartbeatResp消息的发送节点与当前节点连通
	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	//MsgHeartbeat消息对应的MsgReadIndex消息id
	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	//遍历readOnly中记录的消息id
	for _, okctx := range ro.readIndexQueue {
		i++
		//查找消息id对应的readIndexStatus实例
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		//将readIndexStatus实例保存到rss数组中
		rss = append(rss, rs)
		if okctx == ctx {
			//查找到指定的MsgReadIndex消息id，则设置found变量并跳出循环
			found = true
			break
		}
	}

	//查找到指定的MsgReadIndex消息id，则清空readOnly中所有在它之前的消息id及相关内容
	if found {
		//清理readOnly.readIndexQueue
		ro.readIndexQueue = ro.readIndexQueue[i:]
		//清理readOnly.pendingReadIndex
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
