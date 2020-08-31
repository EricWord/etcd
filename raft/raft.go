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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"etcd-with-comments/raft/confchange"
	"etcd-with-comments/raft/quorum"
	pb "etcd-with-comments/raft/raftpb"
	"etcd-with-comments/raft/tracker"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	//当前节点的ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	//记录了集群中所有节点的ID
	peers []uint64

	// learners contains the IDs of all learner nodes (including self if the
	// local node is a learner) in the raft cluster. learners only receives
	// entries from the leader node. It does not vote or promote itself.
	learners []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	//用于初始化raft.electionTimeout
	//逻辑时钟连续推进多少次后，就会触发Follower节点的状态切换及新一轮的leader选举
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	//用于初始化raft.heartbeatTimeout
	//逻辑时钟连续推进多少次后，就触发节点发送心跳消息
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	//当前节点保存raft日志记录使用的存储
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.

	//当前已经应用的记录位置
	Applied uint64

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.

	//用于初始化raft.maxMsgSize字段
	//每条消息的最大字节数
	//如果是math.MaxUint64则没有上限
	//如果是0则则表示每条消息最多携带一条Entry
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?

	//用于初始化raft.maxInflight
	//已经发送出去且未收到响应的最大消息个数
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.

	//是否开启CheckQuorum
	//用于初始化raft.checkQuorum字段
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.

	//是否开启PreVote模式
	//用于初始化raft.preVote字段
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.

	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	//当前节点在集群中的id
	id uint64

	//当前任期号
	//如果Message的Term字段值为0
	//表示该消息是本地消息
	Term uint64
	//当前任期中当前节点将选票投给了那个节点
	//未投票时该节点为None
	Vote uint64

	readStates []ReadState

	// the log
	//在Raft协议中的每个节点都会记录本地Log
	//该字段还涉及日志的缓存
	raftLog *raftLog

	//单条消息的最大字节数
	maxMsgSize         uint64
	maxUncommittedSize uint64
	// TODO(tbg): rename to trk.
	//
	prs tracker.ProgressTracker

	//当前节点在集群中的角色
	state StateType

	// isLearner is true if the local raft node is a learner.
	isLearner bool

	//缓存了当前节点等待发送的消息
	msgs []pb.Message

	// the leader id
	//当前集群中Leader节点的id
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.

	//用于集群中Leader节点的转移
	//记录了此次Leader角色转移的目标节点的ID
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize uint64

	//与只读请求相关
	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.

	//选举计时器的指针
	//单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会增加1
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.

	//心跳计时器的指针
	//单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值会增加1
	heartbeatElapsed int

	//每隔一段时间，Leader节点会尝试连接集群中的其他节点(发送心跳信息)
	//如果发现自己可以连接到的节点个数没有超过半数
	//则主动切换成follower状态
	checkQuorum bool

	//在Follower节点准备发起一次选举之前，会先连接集群中的其他节点
	//并询问它们是否愿意参与选举
	//如果集群中的其他节点能够正常收到Leader节点的心跳消息
	//则会拒绝参与选举，反之参与选举
	//在PreVote过程中，有超过半数的节点响应并参与新一轮选举，则可以发起新一轮的选举
	preVote bool

	//心跳超时时间，heartbeatElapsed达到该值时，就会触发Leader节点发送一条心跳消息
	heartbeatTimeout int
	//选举超时时间
	//当electionElapsed达到该值时，会触发新一轮的选举
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.

	//范围 [electiontimeout, 2 * electiontimeout - 1]
	//当electionElapsed超过该值时即为超时
	randomizedElectionTimeout int
	disableProposalForwarding bool

	//当前节点推进逻辑时钟的函数
	//如果当前节点是Leader,则指向tickHeartbeat()函数
	//如果当前节点是follower或candidate,则指向tickElection
	tick func()
	//当前节点收到消息时的处理函数
	//如果是leader节点，该字段指向stepLeader()函数
	//如果是follower节点,该字段指向stepFollower()函数
	//如果是处于preVote阶段的节点或者是candidate节点，指向stepCandidate()函数
	step stepFunc

	logger Logger
}

//根据传入的config实例中携带的参数创建raft实例并初始化raft使用到的相关组件
func newRaft(c *Config) *raft {
	//检查参数的合法性
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	//创建raftLog实例，用于记录Entry记录
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	//获取raftLog.storage的初始状态(HardState和ConfState)
	//storage的初始状态是通过本地Entry记录回放得到的
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	if len(c.peers) > 0 || len(c.learners) > 0 {
		if len(cs.Voters) > 0 || len(cs.Learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Voters, Learners)")
		}
		cs.Voters = c.peers
		cs.Learners = c.learners
	}

	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,
		prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}

	//初始化raft.prs字段，这里会根据集群中节点的id为每个节点初始化Progress实例
	//注意只有leader节点的raft.prs字段是有效的
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	//根据从storage中获取的HardState初始化raftLog.committed字段
	//以及raft.term和vote字段

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	//如果config中配置了applied，则将raftLog.applied字段重置为指定的applied值
	//上层模块自己控制正确的应用位置时使用该配置

	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	//当前节点切换为follower
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// send persists state to stable storage and then sends to its mailbox.
//在消息发送之前对不同类型的消息进行合法性检查，然后将待发送的消息追加到raft.msg字段中
func (r *raft) send(m pb.Message) {
	//设置消息的发送节点id，即当前节点的id
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		//对消息的term字段进行检查
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		//对其他类型消息的term字段值进行设置
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		//除了MsgProp和MsgReadIndex两类消息(这两类消息的term值为0，即为本地消息)之外，其他类型消息的term字段值在这里同一设置
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	//将消息添加到r.msgs切片中等待发送
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
//向指定目标节点发送MsgApp消息(或MsgSnap消息)
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.prs.Progress[to]
	//检查当前节点是否可以向目标节点发送消息
	if pr.IsPaused() {
		return false
	}
	//创建待发送的消息
	m := pb.Message{}
	//设置目标节点的id
	m.To = to

	//根据当前leader节点记录的next查找发往指定节点的entry记录（ents）
	//及next索引对应的记录的term值
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	//上述两次raftLog查找出现异常时，就会形成MsgSnap消息，将快照数据发送到指定节点
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		//	从当前的leader节点的角度看，目标follower节点已经不再存活
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		//设置消息类型，为后续发送快照数据做准备
		m.Type = pb.MsgSnap
		//获取快照数据
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		//如果获取快照数据出现异常
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		//获取快照的相关信息
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		//将目标follower节点对应的Progress切换成StateSnapshot状态
		//其中会用Progress.PendingSnapshot字段记录快照数据的信息
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		//设置消息的类型
		m.Type = pb.MsgApp
		//设置MsgApp消息的Index字段
		m.Index = pr.Next - 1
		//设置MsgApp消息的LogTerm字段
		m.LogTerm = term
		//设置消息携带的Entry记录集合
		m.Entries = ents
		//设置消息的Commit字段，即当前节点的raftLog中最后一条已提交的记录索引值
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			//根据目标节点对应的Progress.State状态决定其发送消息后的行为
			switch pr.State {
			// optimistically increase the next when in StateReplicate
			case tracker.StateReplicate:
				last := m.Entries[n-1].Index
				//更新目标节点对应的next值(这里不会更新match值)
				pr.OptimisticUpdate(last)
				//记录已发送但是未收到响应的消息
				pr.Inflights.Add(last)
			case tracker.StateProbe:
				//消息发送后，暂停后续消息的发送
				pr.ProbeSent = true
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	//发送前面创建的MsgApp消息
	//send()方法会设置MsgApp消息的term字段值
	//并将其追加到raft.msgs中等待发送
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	//注意MsgHeartbeat消息中Commit字段的设置
	//这主要是因为在发送该消息时，follower节点并不一定已经收到了全部已提交的entry记录
	commit := min(r.prs.Progress[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
//向集群中的其他节点发送MsgApp消息(或MsgSnap消息)
func (r *raft) bcastAppend() {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		//过滤当前节点本身，只向集群中其他节点发送消息
		if id == r.id {
			return
		}
		//向指定节点发送消息
		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

//向集群中其他节点发送MsgHeartbeat消息
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		//过滤当前节点自身
		if id == r.id {
			return
		}
		//向指定的节点发送MsgBeat消息
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) advance(rd Ready) {
	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if index := rd.appliedCursor(); index > 0 {
		r.raftLog.appliedTo(index)
		if r.prs.Config.AutoLeave && index >= r.pendingConfIndex && r.state == StateLeader {
			// If the current (and most recent, at least for this leader's term)
			// configuration should be auto-left, initiate that now.
			ccdata, err := (&pb.ConfChangeV2{}).Marshal()
			if err != nil {
				panic(err)
			}
			ent := pb.Entry{
				Type: pb.EntryConfChangeV2,
				Data: ccdata,
			}
			if !r.appendEntry(ent) {
				// If we could not append the entry, bump the pending conf index
				// so that we'll try again later.
				//
				// TODO(tbg): test this case.
				r.pendingConfIndex = r.raftLog.lastIndex()
			} else {
				r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
			}
		}
	}
	r.reduceUncommittedSize(rd.CommittedEntries)

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	mci := r.prs.Committed()
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		//重置term字段(更新)
		r.Term = term
		//重置vote字段
		r.Vote = None
	}
	//清空lead字段
	r.lead = None

	//重置选举计时器和心跳计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	//重置选举计时器的过期时间(随机值)
	r.resetRandomizedElectionTimeout()

	//清空leadTransferee
	r.abortLeaderTransfer()

	//重置votes
	r.prs.ResetVotes()
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.prs.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			//将当前节点对应的pr.Match设置成lastIndex
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	//只读请求的相关设置
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	//获取raftLog中最后一条记录的索引值
	li := r.raftLog.lastIndex()
	//设置待追加的entry记录的term值和index值
	for i := range es {
		//entry记录的term指定为当前leader节点的任期号
		es[i].Term = r.Term
		//为日志记录指定index
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	//向当前节点的raftLog中追加entry记录
	li = r.raftLog.append(es...)
	//更新当前节点的progress实例
	r.prs.Progress[r.id].MaybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	//尝试提交entry记录，即修改raftLog.committed字段的值
	r.maybeCommit()
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
//推进electionElapsed并检查是否超时
func (r *raft) tickElection() {
	//递增electionElapsed
	r.electionElapsed++

	//promotable（）方法会检查prs字段中是否还存在当前节点对应的Progress实例
	//这是为了检查当前节点是否被从集群中移除了
	//pastElectionTimeout（）方法检查当前的选举计时器是否超时
	if r.promotable() && r.pastElectionTimeout() {
		//重置electionElapsed
		r.electionElapsed = 0
		//发起选举
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	//递增心跳计时器
	r.heartbeatElapsed++
	//递增选举计时器
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		//重置选举计时器，leader节点不会主动发起选举
		r.electionElapsed = 0
		//进行多数检测
		if r.checkQuorum {
			//当选举计时器超过electionTimeout时，会触发一次checkQuorum操作
			//该操作并不会发送网络消息(虽然这里会通过raft.Step()方法处理消息)
			//它只是检查当前节点是否与集群中的大部分节点连通
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		//选举计时器处于electionTimeout~randomizedelectionTimeout~randomizedElectionTimeout时间段时
		//不能进行leader节点的转移
		if r.state == StateLeader && r.leadTransferee != None {
			//放弃转移
			r.abortLeaderTransfer()
		}
	}

	//检查当前节点是否为leader节点
	if r.state != StateLeader {
		return
	}

	//心跳计时器超时
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		//重置心跳计时器
		r.heartbeatElapsed = 0
		//发送MsgBeat消息
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	//重置raft实例term、vote等字段
	r.reset(term)
	//将tick字段设置成tickElection函数
	r.tick = r.tickElection
	//设置当前集群的leader节点
	r.lead = lead
	//设置当前节点的角色
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

//当节点可以连接到集群中半数以上的节点时，会调用该方法切换到Candidate状态
func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//检查当前节点的状态，禁止直接从leader状态切换成candidate状态
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	//重置raft实例 term、vote等字段
	//这里term已经自增
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	//在此次选举中，Candidate节点会将选票投给自己
	r.Vote = r.id
	//修改当前节点的角色
	//注意 切换成Candidate状态之后，raft.leader字段为None，这与其他状态不一样
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

//如果当前集群开启了PreVote模式
//当follower节点的选举计时器超时时，会先调用becomePreCandidate()方法切换到PreCandidate状态
func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//检查当前节点的状态，禁止直接从leader状态切换到PreCandidate状态
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	//将step字段设置成stepCandidate
	r.step = stepCandidate
	r.prs.ResetVotes()
	r.tick = r.tickElection
	r.lead = None
	//修改当前节点的角色
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

//当Candidate节点得到集群中半数以上节点的选票时，会调用该方法切换成leader状态
func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//检查当前节点的状态，禁止直接从follower状态切换成leader状态
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	//将lead字段设置为当前节点的id
	r.lead = r.id
	//更新当前节点的角色
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	r.prs.Progress[r.id].BecomeReplicate()

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
//除了完成状态切换还会向集群的其他节点发送相应类型的消息
//例如 如果当前follower节点要切换成PreCandidate状态，则会发送MsgPreVote消息
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	//在该方法的最后，会发送一条消息,这里term和voteMsg分别用于确定该消息的term值和类型
	var term uint64
	var voteMsg pb.MessageType
	//切换的目标状态是PreCandidate
	if t == campaignPreElection {
		//将当前节点切换成PreCandidate状态
		r.becomePreCandidate()
		//确定最后发送的消息是MsgPreVote类型
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		//确定最后发送消息的term值，这里只是增加了消息的term值，并未增加raft.term字段的值
		term = r.Term + 1
	} else {
		//切换的目标状态是Candidate
		//将当前的节点切换成Candidate状态
		//增加raft.term字段的值，并将当前节点的选票投给自身
		r.becomeCandidate()
		//确定最后发送的消息是MsgPreVote类型
		voteMsg = pb.MsgVote
		//确定最后发送消息的term值
		term = r.Term
	}
	//统计当前节点收到的选票，并统计其所得选票是否超过半数，这次检测主要是为单节点设置的
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		//当得到足够多的选票时，则将PreCandidate状态的节点切换成Candidate状态
		//Candidate状态的节点切换成leader状态
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	var ids []uint64
	{
		idMap := r.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	//状态切换完成后，当前节点会向集群中所有节点发送指定类型的消息
	for _, id := range ids {
		//跳过当前节点自身
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		//在进行leader节点转移时，MsgPreVote或MsgVote消息会在Context字段设置该特殊值
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		//发送执行类型的消息，其中index和logTerm分别是当前节点的raftLog中最后一条消息的Index和term值
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	//记录集群中其他节点的投票结果
	r.prs.RecordVote(id, v)
	//统计结果并返回
	return r.prs.TallyVotes()
}

func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	//首先根据消息的term值进行分类处理
	switch {
	//对本地消息并没有做什么处理
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		//这里只对MsgVote和MsgPreVote两种类型消息进行处理
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			//根据消息的Context字段判断收到的MsgPreVote(或MsgVote)消息是否为leader
			//节点转移场景下产生的，如果是，则强制当前节点参与本次预选(或选举)
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			//下面通过一系列条件判断当前节点是否参与此次选举(或预选)，其中主要检查是否开启了CheckQuorum模式、
			//当前节点是否有已知的lead节点以及其选举计时器的时间
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				//当前节点不参与此次选举
				return nil
			}
		}
		//在这个switch中，当前节点会根据消息类型决定是否切换状态
		switch {
		//收到MsgPreVote消息时，不会引起当前节点的切换
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	//根据message的type进行分类处理
	switch m.Type {
	case pb.MsgHup:
		//只有非leader节点才会处理msghub消息
		if r.state != StateLeader {
			if !r.promotable() {
				r.logger.Warningf("%x is unpromotable and can not campaign; ignoring MsgHup", r.id)
				return nil
			}
			//获取raftLog中已提交但未应用的entry记录
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			//检查是否有未应用的EntryConfChange记录，如果有就放弃发起选举的机会
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			//检查当前集群是否开启了preVote模式
			//如果开启了，则先调用campaign()方吗切换当前节点的角色，发起preVote
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			//如果当前节点已经是leader节点，则仅仅输出一条debug日志
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		//当前节点在参与预选时，会综合下面几个条件决定是否投票
		//1、当前节点是否已经投过票
		//2、MsgPreVote消息发送者的任期号是否更大
		//3、当前节点投票给了对方节点
		//4、MsgPreVote消息发送者的raftLog中是否包含当前节点的全部entry记录

		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd-with-comments/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			//将票投给MsgPreVote消息的发送节点
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			//不满足上述投赞同票条件时，当前节点会返回拒绝票(即响应消息中的Reject字段会设置成true)
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		//向所有节点发送心跳信息
		r.bcastHeartbeat()
		return nil
	case pb.MsgCheckQuorum:
		// The leader should always see itself as active. As a precaution, handle
		// the case in which the leader isn't in the configuration any more (for
		// example if it just removed itself).
		//
		// TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
		// leader steps down when removing itself. I might be missing something.
		if pr := r.prs.Progress[r.id]; pr != nil {
			pr.RecentActive = true
		}
		//用于检查leader节点是否与集群中大部分节点连通
		//如果不连通则切换成follower状态
		if !r.prs.QuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp:
		//检查MsgProp消息中是否携带了Entry记录
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		//检查当前节点是否被移出集群
		//如果当前节点以leader状态被移出集群
		//则不再处理MsgProp消息
		if r.prs.Progress[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		//检查当前是否正在进行leader节点的转移，如果是不再处理MsgProp消息
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		//遍历MsgProp消息携带的全部entry记录
		for i := range m.Entries {
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			//如果存在EntryConfChange类型的Entry记录
			if e.Type == pb.EntryConfChange {
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil {
				alreadyPending := r.pendingConfIndex > r.raftLog.applied
				alreadyJoint := len(r.prs.Config.Voters[1]) > 0
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0

				var refused string
				if alreadyPending {
					refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint {
					refused = "must transition out of joint config first"
				} else if !alreadyJoint && wantsLeaveJoint {
					refused = "not in joint state; refusing empty conf change"
				}

				if refused != "" {
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}

		//将上述entry记录追加到当前节点的raftLog中
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	case pb.MsgReadIndex:
		// If more than the local vote is needed, go through a full broadcast,
		// otherwise optimize.
		//集群场景
		if !r.prs.IsSingleton() {
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return nil
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			//leader节点检查自身在当前任期中是否已提交过Entry记录
			//如果没有，则无法进行读取操作
			switch r.readOnly.option {
			//两种处理读请求的模式
			case ReadOnlySafe:
				//记录当前节点的raftLog.committed字段值，即已提交的位置
				r.readOnly.addRequest(r.raftLog.committed, m)
				// The local node automatically acks the request.
				r.readOnly.recvAck(r.id, m.Entries[0].Data)
				//发送心跳
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				//该模式下，leader节点直接并不会发送任何消息来确认自身身份
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: ri, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {                                  // only one voting member (the leader) in the cluster
			//单节点情况
			if m.From == None || m.From == r.id { // from leader itself
				r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
			} else { // from learner member
				r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: r.raftLog.committed, Entries: m.Entries})
			}
		}

		return nil
	}

	// All other message types require a progress for m.From (pr).
	//根据消息的from字段获取对应的Progress实例，为后面的消息处理做准备
	pr := r.prs.Progress[m.From]
	if pr == nil {
		//如果没有操作到对应的Progress实例(该节点可能已从集群中删除)时，直接返回
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		//更新RecentActive
		//从leader节点的角度看，MsgAppResp消息的发送节点还是存活的
		pr.RecentActive = true

		//MsgAp消息被拒绝
		if m.Reject {
			r.logger.Debugf("%x received MsgAppResp(MsgApp was rejected, lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			//通过MsgAppResp消息携带的信息及对应的Progress状态，重新设置其Next值
			if pr.MaybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate {
					//如果对应的Progress处于StateReplicate状态则切换成StateProbe状态，试探Follower的匹配位置
					//这里的试探是指发送一条消息并等待其响应之后再发送后续的消息
					pr.BecomeProbe()
				}
				//再次向对应follower节点发送MsgApp消息
				r.sendAppend(m.From)
			}
		} else {
			//之前发送的MsgApp消息已经被对应的follower节点接收(Entry记录被成功追加)
			oldPaused := pr.IsPaused()
			//MsgAppResp消息的Index字段是对应follower节点raftLog中最后一条Entry记录的索引
			//这里会根据该值更新其对应的Progress实例的Match和next
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					//一旦MsgApp被follower节点接收，则表示已经找到其正确的next和match
					//不必再进行"试探"，这里将对应的Progress.state切换成StateReplicate
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					//之前由于某些原因，leader节点通过发送快照的方式恢复follower节点，
					//但在发送MsgSnap消息的过程中，Follower节点恢复，并正常接收了leader节点的MsgApp消息
					//此时会丢弃MsgApp消息，并开始"试探"该follower节点正确的match和next值
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					//之前向某个follower节点发送MsgApp消息时，会将其相关信息保存到对应的
					//Progress.ins中，在这里收到相应的MsgAppResp响应之后，会将其
					//从ins中删除，这样可以实现限流的效果，避免网络出现延迟，继续发送消息，从而导致网络更加拥堵
					pr.Inflights.FreeLE(m.Index)
				}

				//收到一个follower节点的MsgAppResp消息之后，除了修改响应的Match和Next值
				//还是尝试更新raftLog.committed,因为有些Entry记录可能在此次复制中被保存到了半数以上的节点中

				if r.maybeCommit() {
					//向所有节点发送MsgApp消息
					//注意此次MsgApp消息的commit字段与上次MsgApp消息已经不同
					r.bcastAppend()
				} else if oldPaused { //之前是pause状态，现在可以任性地发送消息了
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					//之前leader节点暂停向该follower节点发送消息，收到MsgAppResp消息后，
					//在上述代码中已经重置了相应状态，所以可以继续发送MsgApp消息
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		//更新对应Progress.RecentActive 表示follower节点与自己连通
		pr.RecentActive = true
		//表示可以继续向follower节点发送消息
		pr.ProbeSent = false

		// free one slot for the full inflights window to allow progress.
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() {
			//释放inflights中第一个消息，这样就可以开始后续消息的发送
			pr.Inflights.FreeFirstOne()
		}
		//当leader节点收到follower节点的MsgHeartbeat消息后，会比较对应的match值与leader节点的raftLog
		//从而判断follower节点是否已拥有了全部的entry记录
		if pr.Match < r.raftLog.lastIndex() {
			//通过向指定节点发送MsgApp消息完成Entry记录的复制
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		//只有won才会继续往下执行
		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			//根据MsgReadIndex消息的from字段，判断该MsgReadIndex消息是否为follower节点转发到leader节点的消息
			if req.From == None || req.From == r.id { // from local member
				//如果是客户端直接发送到leader节点的消息，则将MsgReadIndex消息对应的已提交位置
				//以及其消息id封装成ReadState实例，添加到raft.readStates中保存
				//后续会有其他goroutine读取该数组，并对相应的MsgReadIndex下次进行响应
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				//如果是其他follower节点转发到leader节点的MsgReadIndex消息，则leader节点会向follower节点返回相应的
				//MsgReadIndexResp消息，并由follower节点响应client
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		//检查follower节点对应的状态是否为StateSnapshot
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		//之前发送MsgApp消息时出现异常
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			//发送MsgSnap消息失败
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		//无论MsgSnap消息发送是否失败，都会将对应Progress切换成StateProbe状态
		//之后单条发送消息进行试探

		//暂停leader节点向该follower节点继续发送消息，如果发送MsgSnap消息成功了，则待leader节点收到相应的
		//响应消息(MsgAppResp消息)，即可继续发送后续MsgApp消息
		//如果发送MsgSnap消息失败了，则leader节点会等到收到MsgHeartbeatResp消息时，才会重新开始发送后续消息
		pr.ProbeSent = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		//当follower节点变得不可达如果继续发送MsgApp消息，则会有大量消息丢失
		if pr.State == tracker.StateReplicate {
			//将follower节点对应的Progress实例切换成StateProbe状态
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
//封装了PreCandidate节点处理消息的行为
func stepCandidate(r *raft, m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	//根据当前节点的状态决定其能够处理的选举响应消息的类型
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case myVoteRespType:
		//记录并统计投票结果
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		//得票是否超过半数
		switch res {
		case quorum.VoteWon:
			if r.state == StatePreCandidate {
				//当PreCandidate节点在预选中收到半数以上的选票之后，会发起正式的选举
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			//赞同票与拒绝票相等时，无法获取半数以上的票数，当前节点切换成follower状态
			//等待下一轮的选举
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

//封装了follower节点处理消息的行为
func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp:
		//当前集群没有leader节点，则忽略该MsgProp消息
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		//将消息的to字段设置为当前leader节点的id
		m.To = r.lead
		//将MsgProp消息发送到当前的leader节点
		r.send(m)
	case pb.MsgApp:
		//重置选举计算器，防止当前follower发起新一轮选举
		r.electionElapsed = 0
		//设置raft.lead记录，保存当前集群的leader节点的id
		r.lead = m.From
		//将MsgApp消息中携带的Entry记录追加到raftLog中，并且向leader节点发送MsgAppResp消息
		//响应此次MsgApp消息
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		//重置选举计时器，防止当前follower节点发起新一轮选举
		r.electionElapsed = 0
		//设置leader节点的id
		r.lead = m.From
		//其中会修改raft.committed字段的值(放心，在leader发送消息时，已经确定了当前follower节点有committed之前的全部日志记录)
		//然后发送MsgHeartbeatResp类型消息，响应此次心跳
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		//重置raft.electionElapsed,防止发生选举
		r.electionElapsed = 0
		//设置leader的id
		r.lead = m.From
		//通过MsgSnap消息中的快照数据，重建当前节点的raftLog
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		if r.promotable() {
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		//检查当前集群的leader节点
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		//将MsgReadIndex消息转发给当前的leader节点
		r.send(m)
	case pb.MsgReadIndexResp:
		//检查MsgReadIndexResp消息的合法性
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		//将MsgReadIndex消息对应的消息id以及已提交位置(raftLog.committed)封装成ReadState实例
		//并添加到raft.readStates中，等待其他goroutine来处理
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

//首先会检查MsgApp消息中携带的Entry记录是否合法
//然后将这些Entry记录追加大到raftLog中，
//最后创建相应的MsgAppresp消息
func (r *raft) handleAppendEntries(m pb.Message) {
	//m.Index 表示leader发送给follower的上一条日志的索引位置
	//如果follower节点在index位置的Entry记录已经提交过了
	//则不能进行追加操作
	//已提交的记录不能被覆盖
	//所以Follower节点会将其committed位置通过MsgAppresp消息(index字段)通知leader节点

	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	//尝试将消息携带的Entry记录追加到raftLog中
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		//如果追加成功，则将最后一条记录的索引值通过MsgAppResp消息返回给leader节点
		//这样leader节点就可以根据此值更新其对应的next和match值
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		//如果追加失败，则将失败信息返回给leader节点，同时还返回一些提示信息
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	//根据MsgHeartbeat消息的Commit字段，更新raftLog中记录的已提交位置
	//注意，在leader节点发送MsgHeartbeat消息时，已经确定了当前follower节点中的raftLog.committed字段的合适位置
	r.raftLog.commitTo(m.Commit)
	//发送MsgHeartbeatResp消息，响应心跳
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}
//读取MsgSnap消息中的快照数据，并重建当前节点的raftLog
func (r *raft) handleSnapshot(m pb.Message) {
	//获取快照数据的元数据
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {//返回值表示是否通过快照数据进行了重建
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		//向leader节点返回MsgAppResp消息
		//该MsgAppResp消息MsgSnap消息的响应
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.state != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouuldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.Metadata.ConfState
	for _, set := range [][]uint64{
		cs.Voters,
		cs.Learners,
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
	}
	//根据快照数据的元数据查找匹配的Entry记录，如果存在，则表示当前节点已经拥有了该快照中的全部数据，所以无须进行后续重建操作
	if !found {
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.

	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		//快照中全部的Entry记录都已经提交了，所以尝试修改当前节点的raftLog.committed
		//raftLog.committed只增不减
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}
	//raftLog.unstable记录该快照数据，同时重置相关字段
	r.raftLog.restore(s)

	// Reset the configuration and add the (potentially updated) peers in anew.
	//清空raft.prs，并根据快照的元数据进行重建
	r.prs = tracker.MakeProgressTracker(r.prs.MaxInflight)
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	pr := r.prs.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.prs.Progress[r.id]
	return pr != nil && !pr.IsLearner
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   r.prs,
			LastIndex: r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	r.prs.Config = cfg
	r.prs.Progress = prs

	r.logger.Infof("%x switched to configuration %s", r.id, r.prs.Config)
	cs := r.prs.ConfState()
	pr, ok := r.prs.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the the leadTransferee was removed, abort the leadership transfer.
	if _, tOK := r.prs.Progress[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
