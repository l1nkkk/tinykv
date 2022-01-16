package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type MsgStep struct {
	msgHandle map[pb.MessageType]func(r *Raft, msg *pb.Message) error
	logger    *log.Logger
}

func CreateMsgStep() *MsgStep {
	var m *MsgStep
	m = &MsgStep{
		msgHandle: map[pb.MessageType]func(r *Raft, msg *pb.Message) error{
			pb.MessageType_MsgHup:         handleMsgHub,
			pb.MessageType_MsgRequestVote: handleMsgVoteReq,
		},
	}
	return m
}

func (m *MsgStep) Step(r *Raft, msg *pb.Message) error {
	return m.msgHandle[msg.MsgType](r, msg)
}

// 处理特殊的msg.Term，即 r.Term != msg.Term
func (m *MsgStep) checkTerm(r *Raft, msg *pb.Message) error {
	if r.Term > msg.Term {
		// TODO 过期的消息，后面对此进行处理
		// etcd 中 r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp)，

	}
}

func handleMsgHub(r *Raft, msg *pb.Message) error {
	switch r.State {
	case StateFollower, StateCandidate:
		// TODO 检查是否所有 config log entries 已经被提交
		return r.campaign()
	case StateLeader:
		r.logger.Info("the node is a leader, it will ignore the MsgHub")
		return nil
	default:
		r.logger.Panic("undefine error")
	}
	return nil
}

func handleMsgVoteReq(r *Raft, msg *pb.Message) error {
	// 1. leader资格判断
	return nil
}
