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
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
//
// 当参数中的 index 由于已被压缩不可用时，返回该错误
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
//
// 这里边的方法如果返回error，那么该 raft 实例变不可用，应用层有责任去清理和恢复这种情况
// l1nkkk: 为什么只有读接口，是因为在 Raft 中只需要读取已经持久化的log就够了，而写的责任
// 交给客户端
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	//
	// 返回已持久化的 硬状态 和 配置信息（CurrentTerm, VoteFor, commitIndex; nodeid）
	// l1nkkk：commitIndex 在 Raft 的论文中为易失的
	InitialState() (pb.HardState, pb.ConfState, error)

	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	//
	// maxSize 参数被删除。返回[lo,hi) 之间的log_entries
	Entries(lo, hi uint64) ([]pb.Entry, error)

	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	//
	// 传入 log 的 index， 返回其 term，i 必须在 [FirstIndex()-1, LastIndex()]
	// 范围内，FirstIndex()-1 也可以检索到，因为snapshot 保留 lastIndex
	Term(i uint64) (uint64, error)

	// LastIndex returns the index of the last entry in the log.
	//
	// 最后一条log_entry 的 index，当 log_entries 为空，返回 snapshot 的 lastindex
	LastIndex() (uint64, error)

	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	//
	// 第一条 log_entry 的 index，当 log_entries 为空，FirstIndex 不可用，
	// 此时返回{0，ErrUnavailable}
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	//
	// 返回最近的快照数据
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	// Append() 运行在 应用层 的goroutine

	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		//
		// l1nkkk ： 注意这里的dumy entry。etcd中没有给 snapshot 赋值一个 dumy snapshot。
		// 初始化的时候，dumy entry 的 index 和 term 都为0
		ents:     make([]pb.Entry, 1),
		snapshot: pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{}}},
	}
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, *ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	// 1. 获取offset: 实质是 recent snapshot's lastIndex
	offset := ms.ents[0].Index

	// 2. 处理越界情况
	if lo <= offset { // 注意这里的 = 号，因为 offset 取自 dumy entry
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 { // 因为取自 [lo, hi) 的log entries，所以允许 hi = lastIndex + 1
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	// 3. 切片获取返回结果
	ents := ms.ents[lo-offset : hi-offset]
	if len(ms.ents) == 1 && len(ents) != 0 {
		// only contains dummy entries.
		return nil, ErrUnavailable
	}
	return ents, nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	// l1nkkk : 这种风格挺好的，自己的方法调用 lastIndex， 用户调用 LastIndex
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	// update by l1nkkk
	if len(ms.ents) == 1 {
		return 0, ErrUnavailable
	}
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {

	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
//
// 利用 snap 覆盖 Storage
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	// 1. 旧快照判断
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	// 2. 清空 entries，插入 dumy entry
	// l1nkkk ques: 之前持久化的 entries 都直接丢了，也不判断下嘛，然后截断
	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// l1nkkk: 以下为用户调用的，非Storage 接口

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
//
// 英文注释值得细读。
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// 1. 如果 i < 当前最新snapshot的lastindex，错误
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	// 2. 如果 i > storage.lastIndex， 错误
	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 3. 更新 ms.snapshot
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		// l1nkkk: 如果传入cs，则在snapshot的元数据中存储 cs（ConfState，Raft节点配置）
		ms.snapshot.Metadata.ConfState = cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
//
// 将 compactIndex 之前的entries释放。应用层有责任保证，compactIndex <= currentApply
//
// l1nkkk: 从这里可以看出 snapshot 都是已经 apply 的了。感觉这个函数是和 CreateSnapshot
// 配合使用的
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 1. 判断越界
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	// 2. 释放 entries
	i := compactIndex - offset
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i) // 类型，len， capo
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// 1. 如果 传入的 entries 已经被 compact，snapshot里的数据已经都是apply的了，
	// 直接忽略处理
	// shortcut if there is no new entry.
	if last < first {
		return nil
	}

	// 2. 如果 传入的 entries 有部分被 compact， 截取传入的entries中剩余没有被compact的部分
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 这时候肯定有 entries[0].Index > ms.ents[0].Index

	// 注：画个图就出来了
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset: // 相交
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset: // 相切
		ms.ents = append(ms.ents, entries...)
	default: // 相离
		log.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
