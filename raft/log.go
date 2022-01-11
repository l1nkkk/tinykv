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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"os"
	"runtime"
)


// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
//
// 用于管理和维护该节点上的 raft 日志数据
// 这里所有未提交的日志数据，都放在了 entries。
// 这样的处理方式与 etcd 不同， 更加的简单。确实etcd的那种方式不太有必要，用内存来实现Storage，
// 还要在内存中划分出 已持久化的数据 和 未持久化的数据，带来了更多的复杂性
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	//
	// 用来存储自上一次快照之后，已经持久化的 log entries
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	//
	// 可提交的 log_entries 的最大 index
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	//
	// apply 到状态机的 log_entries 的 最大 index
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	//
	// stabled 表示在 entries 切片中，index <= stabled 的日志都被持久化到Storage中了
	stabled uint64

	// all entries that have not yet compact.
	//
	// 所有还没被压缩的 entries
	entries []pb.Entry

	// pendingSnapshot the incoming unstable snapshot, if any.
	//
	// 刚同步过来，未持久化的日志快照，2C中才用到
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	logger *log.Logger
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil{
		log.Panic("storage must not be nil")
	}
	var(
		err error
		rtn *RaftLog
		lastIndex uint64
		recentSnap pb.Snapshot
	)
	// 1. 构造实例
	rtn = &RaftLog{
		storage:         storage,
		pendingSnapshot: nil,
	}

	// 2. 初始化 committed applied stabled
	if lastIndex,err = rtn.storage.LastIndex(); err != nil{
		log.Panic("newLog() error in storage.LastIndex()")
	}

	if recentSnap, err = rtn.storage.Snapshot(); err != nil{
		log.Panic("newLog() error in storage.FirstIndex")
	}


	// 已经持久化了不一定已经commit、apply
	rtn.stabled = lastIndex
	rtn.committed = recentSnap.Metadata.Index
	rtn.applied = recentSnap.Metadata.Index

	// 3. dummy entry
	// l1nkkk: 这里我也借鉴了下 Storage， 给切片传一个 dummy entries
	if rtn.entries, err = storage.Entries(recentSnap.Metadata.Index, lastIndex+1); err != nil{
		log.Panic("newLog() error in storage.Entries()")
	}
	rtn.entries = append([]pb.Entry{{
		EntryType: pb.EntryType_EntryNormal,
		Term: recentSnap.Metadata.Term,
		Index: recentSnap.Metadata.Index,
		Data: nil,
	}},rtn.entries...)

	// 4. 初始化 log
	rtn.logger = log.NewLogger(os.Stderr, "RaftLog: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetHighlighting(runtime.GOOS != "windows")


	return rtn
}

// add by l1nkkk
// l1nkkk 易于调试
func (l *RaftLog)String() string{
	l.check()
	return fmt.Sprintf("RaftLog: commitIndex=%d, applyIndex=%d, unstableIndex=%d, uncompactLen=%d" +
		", stableLen:unstableLen=%d:%d", l.committed, l.applied, l.stabled+1, len(l.entries),
		l.stabled - l.entries[0].Index, l.entries[len(l.entries)-1].Index - l.stabled)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	l.check()
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	l.check()
	//stablePos := l.stabled - l.entries[0].Index
	return l.entries[1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.check()
	var (
		err error
	)
	//commitPos := l.committed - l.entries[0].Index
	//applyPos := l.applied - l.entries[0].Index
	//return l.entries[applyPos+1:commitPos+1]

	if ents, err = l.storage.Entries(l.applied, l.committed); err != nil{
		l.logger.Panic("storage.Entries() error")
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	l.check()
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
//
// 返回0表示获取失败，err 永远返回的都是nil
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	l.check()
	// 1. i > stable, unstable
	if i > l.stabled{
		if uint64(len(l.entries)) > i - l.stabled {
			return l.entries[i-l.stabled].Index, nil
		}
		// 越界
		return 0, nil
	}
	// 2. i <=stable, storage
	return l.storage.Term(i)
}

// add by l1nkkk
func (l *RaftLog)check(){
	if l == nil{
		log.Panic("RaftLog must not be nil")
	}
	var (
		firstIndex uint64
		lastIndex uint64
		err error
	)

	if firstIndex,err = l.storage.FirstIndex(); err != nil{
		l.logger.Panic("storage.FirstIndex() error")
	}

	lastIndex = l.entries[len(l.entries)-1].Index

	if !(firstIndex <= l.applied && l.applied <= l.committed &&
		l.committed <= l.stabled && l.stabled <= lastIndex){
		l.logger.Panic("Error status: ", l)
	}
}
