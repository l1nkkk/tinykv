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
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"os"
	"runtime"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None 表示占位符节点，但没有leader的时候
//
// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType 节点身份状态，Follower or Candidate or Leader
//
// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped propose 失败，返回该error，
// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config 节点的配置信息，包含用于启动Raft的参数
//
// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	//
	// 节点ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	//
	// 集群的节点id集合
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	//
	// ElectionTick 节点的选举超时时间，必须 >> HeartbeatTick 心跳时间
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	//
	// HeartbeatTick leader节点发送心跳包的时间间隔
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	//
	// Storage 负责对 日志数据 和 节点硬状态进行持久化
	Storage Storage

	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	//
	// apply 到状态机的最后一个log entry 的 index
	Applied uint64
}

// 检验 Config 对象的合法性
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

	// add by l1nkkk
	// 不能同时出现 peers 和 stroge 中的 ConfState
	if len(c.peers) != 0 {
		if _, cs, err := c.Storage.InitialState(); err != nil {
			return err
		} else {
			if len(cs.Nodes) != 0 {
				return errors.New("conflict between peers and Storage")
			}
		}
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
//
// 只用在leader节点中，用于维护 follower 的状态
type Progress struct {
	Match, Next uint64
}

// Raft raft算法的核心
type Raft struct {
	// Raft节点id
	id uint64

	// Term 当前节点已知的最新任期
	Term uint64
	// Vote 当前任期，该节点投票给了哪个节点（id），如果没有就为0
	Vote uint64

	// RaftLog 管理日志数据
	RaftLog *RaftLog

	// Prs 管理 follower 节点的状态，只有当该 raft 节点为 leader 的时候，才有用。
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	//
	// 投票记录
	votes map[uint64]bool

	// msgs need to send
	//
	// msgs 需要发送出去的消息
	msgs []pb.Message

	// the leader id
	//
	// 当前集群中leader的id
	Lead uint64

	// heartbeat interval, should send
	//
	// 心跳周期
	heartbeatTimeout int

	// baseline of election interval
	//
	// 选举超时时间
	electionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	//
	// heartbeatElapsed 当前的心跳计时器，表示距离上一次心跳已经过了多少ticks.
	// 只有leader才会使用
	heartbeatElapsed int

	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	//
	// electionElapsed 选举超时计时器，如果是leader or candidate 表示上次选举超时到现在的ticks；
	// 如果是 follower 表示上次选举超时或上次收到valid message 当现在的ticks。
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	//
	// leadTransferee leader 禅让的目标id
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	//
	// PendingConfIndex 用于配置动态变更，暂时不管，这是3a的
	PendingConfIndex uint64

	// ====add by l1nkkk
	msgStep             *MsgStep
	randElectionTimeout int
	logger              *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	var (
		r         *Raft
		hardState pb.HardState
		confState pb.ConfState
		peers     []uint64
		err       error
	)
	if err = c.validate(); err != nil {
		panic(err.Error())
	}
	if hardState, confState, err = c.Storage.InitialState(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	// 1. 初始化固定的几个变量
	r = &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		State:            StateFollower, // 刚启动先将其设置为 follower
		Lead:             None,
	}

	// 2. 初始化RaftLog
	r.RaftLog = newLog(c.Storage)
	r.RaftLog.applied = c.Applied
	r.RaftLog.committed = hardState.Commit

	// 3. 初始化process
	r.Prs = make(map[uint64]*Progress)
	if len(c.peers) != 0 {
		peers = c.peers
	} else {
		peers = confState.Nodes
	}
	for _, id := range peers {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}

	// 4. init logger
	r.logger = log.NewLogger(os.Stderr, "RaftLog: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetHighlighting(runtime.GOOS != "windows")

	// 5. init msgStep
	r.msgStep = CreateMsgStep()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
//
// 向节点发送 addMsg
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetState(term, lead, StateFollower)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetState(r.Term+1, None, StateCandidate)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 1. reset State
	r.resetState(r.Term, r.id, StateLeader)

	// TODO: 2. propose dumy entries
	//r.RaftLog.
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// l1nkkk: etcd 中 Raft 承担了太多职责，这里把处理 msg 的职责分了出去
	return r.msgStep.Step(r, &m)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// ============add by l1nkkk

func (r *Raft) stepLeader(m *pb.Message) {
	switch {

	}
}

func (r *Raft) stepFolloer(m *pb.Message)   {}
func (r *Raft) stepCandidate(m *pb.Message) {}

// 竞选 leader
func (r *Raft) campaign() error {
	// 1. become candidate
	r.becomeCandidate()

	// 2-1. 如果是单节点，直接成为 leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}

	// 2-2. 群发preVote
	for toid := range r.Prs {
		if toid == r.id {
			continue
		}
		r.send(pb.Message{
			From:    r.id,
			To:      toid,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   r.RaftLog.LastIndex(),
			Term:    r.RaftLog.LastTerm(),
		})
	}
}

// 设置 msg 的 term， 并将其 append 到 msg slice。
// 所有发送的日志都调用这个，可以统一做一些管理，比如日志。
func (r *Raft) send(msg pb.Message) {
	r.logger.Debugf("Addend msg in node%d ：%v", r.id, msg.String())
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

// 重置raft状态
func (r *Raft) resetState(term uint64, newLead uint64, s StateType) {

	// 1. update term 、voteFor of hardState
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	// 2. update value about time
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandElectionTimeout()

	// 3. clear vote record and currentLead
	r.votes = make(map[uint64]bool)
	r.Lead = newLead

	r.State = s

	// 4. reset progresses？
	//for _, p := range r.Prs {
	//	p.Next = r
	//}

}

func (r *Raft) resetRandElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + getRandWrap().Intn(r.electionTimeout)
}
