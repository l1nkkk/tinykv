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
// 用于管理和维护该节点上的 raft 日志数据，
// 这里所有未持久化的日志数据，都放在了 entries。
// 这样的处理方式与 etcd 不同， 更加的简单。
// Storage 只是持久化数据的内存映像
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	//
	// 用来存储自上一次快照之后，已经持久化的 log entries
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	//
	// commitIndex
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	//
	// applyIndex
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
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	var (
		err        error
		rtn        *RaftLog
		lastIndex  uint64
		recentSnap pb.Snapshot
	)
	// 1. 构造实例
	rtn = &RaftLog{
		storage:         storage,
		pendingSnapshot: nil,
	}

	// 2. 初始化 committed applied stabled
	// TODO: snapshot is enpty, how to deal this situation
	if lastIndex, err = rtn.storage.LastIndex(); err != nil {
		log.Panic("newLog() error in storage.LastIndex()")
	}
	if recentSnap, err = rtn.storage.Snapshot(); err != nil {
		log.Panic("newLog() error in storage.FirstIndex")
	}
	// 已经持久化了不一定已经commit、apply
	rtn.stabled = lastIndex
	rtn.committed = recentSnap.Metadata.Index
	rtn.applied = recentSnap.Metadata.Index

	// 3. dummy entry
	// l1nkkk: 这里我也借鉴了下 Storage， 给切片传一个 dummy entries
	if rtn.entries, err = storage.Entries(recentSnap.Metadata.Index+1, lastIndex+1); err != nil {
		log.Panic("newLog() error in storage.Entries()")
	}
	rtn.entries = append([]pb.Entry{{
		EntryType: pb.EntryType_EntryNormal,
		Term:      recentSnap.Metadata.Term,
		Index:     recentSnap.Metadata.Index,
		Data:      nil,
	}}, rtn.entries...)

	// 4. 初始化 log
	rtn.logger = log.NewLogger(os.Stderr, "RaftLog: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetHighlighting(runtime.GOOS != "windows")

	return rtn
}

// add by l1nkkk.
// l1nkkk 易于调试
func (l *RaftLog) String() string {
	l.check()
	return fmt.Sprintf("RaftLog: commitIndex=%d, applyIndex=%d, unstableIndex=%d, uncompactLen=%d"+
		", stableLen:unstableLen=%d:%d", l.committed, l.applied, l.stabled+1, len(l.entries),
		l.stabled-l.entries[0].Index, l.entries[len(l.entries)-1].Index-l.stabled)
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
	return l.entries[l.stabled-l.entries[0].Index:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.check()

	commitPos := l.committed - l.entries[0].Index
	applyPos := l.applied - l.entries[0].Index
	return l.entries[applyPos+1 : commitPos+1]

	//if ents, err = l.storage.Entries(l.applied, l.committed); err != nil {
	//	l.logger.Panic("storage.Entries() error")
	//}
	//return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	l.check()
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
//
// 失败返回{0,nil}, err 永远为 nil
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	l.check()
	// 1. check index
	if i <= 0 {
		return 0, nil
	}
	if l.entries[0].Index > i || i > l.LastIndex() {
		return 0, nil
	}

	// 2. i <=stable, storage
	return l.entries[i-l.entries[0].Index].Index, nil
}

// add by l1nkkk
func (l *RaftLog) check() {
	if l == nil {
		log.Panic("RaftLog must not be nil")
	}
	var (
		firstIndex uint64
		lastIndex  uint64
		err        error
	)

	if firstIndex, err = l.storage.FirstIndex(); err != nil {
		if err != ErrUnavailable {
			l.logger.Panic("storage.FirstIndex() undefine error")
		}
	}

	lastIndex = l.entries[len(l.entries)-1].Index

	if !(firstIndex <= l.applied && l.applied <= l.committed &&
		l.committed <= l.stabled && l.stabled <= lastIndex) {
		l.logger.Panic("Error status: ", l)
	}
}

// ====== add by l1nkkk

// LastTerm 返回最后entry的term值，错误或没有日志则返回0
func (l *RaftLog) LastTerm() uint64 {
	if rtn, err := l.Term(l.LastIndex()); err != nil {
		l.logger.Panic(err)
	} else {
		return rtn
	}
	return 0
}

// AppendWithCheck 追加 log entries，会对其一致性进行检查，主要用于 appMsg同步过来的log entries。
// 寻找 ents 中实际 appen 到raft 中的部分，对其进行截取后append
func (l *RaftLog) appendWithCheck(preLogIndex, preLogTerm uint64, ents ...pb.Entry) (newLastIndex uint64, ok bool) {
	// 1. RPC一致性检测
	if l.checkTerm(preLogIndex, preLogTerm) {
		// 2. get insert point
		p := l.findConflict(ents)

		// 3. append log，check conflict with commitIndex
		switch {
		case p == 0:
		case p <= l.committed:
			l.logger.Panic("entry %d conflict with committed entry [committed(%d)]", p, l.committed)
		default:
			l.append(ents[p-ents[0].Index:]...)
		}
	}
	return 0, false
}

// Append 往 RaftLog.entries 中灌入数据，不需管 RaftLog.storage 中的数据。
// 灌入后交给 Ready 通知应用层，应用层修改 RaftLog.storage 中的数据。
func (l *RaftLog) append(ents ...pb.Entry) (newLastIndex uint64) {

	// 1. check ents
	if len(ents) == 0 {
		return l.LastIndex()
	}

	appIndex := ents[0].Index
	if appIndex <= l.committed {
		l.logger.Errorf("conflict index between appEntries(%d) and current commitIndex(%d)", appIndex, l.committed)
	}

	// 2. push to current entries
	l.truncateAndAppend(ents)
	return l.LastIndex()
}

// TruncateAndAppend 将 ents 覆盖append到 raft 中，已经假定 ents 为合法数据，
// 即不会覆盖已经提交的数据，不做过多的检查。
func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	appIndex := ents[0].Index
	switch {
	case appIndex == l.LastIndex()+1: // 1. 正好相切，直接追加
		l.entries = append(l.entries, ents...)
	case appIndex > l.committed && appIndex <= l.LastIndex(): // 2. 有覆盖的情况
		// 保留部分 + 覆盖部分
		l.entries = append(l.entries[1:appIndex-l.entries[0].Index], ents...)
	default: // 3. 异常情况，这些情况应该再调用前被过滤
		l.logger.Panic("undefine situation")
	}

}

func (l *RaftLog) checkTerm(preLogIndex, preLogTerm uint64) bool {
	if preLogIndex < l.LastIndex() {
		if t, err := l.Term(preLogIndex); err != nil && t != preLogTerm {
			return false
		}
		return true
	}
	return false
}

// 传入待插入的log entries，返回第一个冲突 或 第一个raftLog中所不存在的index；
// 因为传入的log entries可能与raftLog中的的log entries有相交的部分，下面分两种情况：
// 1. 存在冲突，则返回第一个冲突的index
// 2. 不存在冲突，则返回只需插入部分的第一个index，特殊的，如果带插入的RaftLog全都有之，则返回0
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {

	for _, ne := range ents {
		if !l.checkTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index // raftLog 中已经没有可以 checkTerm的entries
		}
	}
	// 不存在冲突，且数据RaftLog都已经拥有
	return 0
}

func (l *RaftLog) zeroTermOnErrCompacted(term uint64, err error) uint64 {
	if err == nil {
		return term
	}
	if err == ErrCompacted {
		return 0
	}

	l.logger.Panic("unexpected error (%v)", err)
	return 0
}
