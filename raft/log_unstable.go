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

import pb "etcd-with-comments/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	offset  uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
//尝试获取unstabled的第一条Entry记录的索引值
//如果失败返回(0,false)
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	//如果unstable记录了快照，则通过快照元数据返回响应的索引值
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
//尝试获取unstable的最后一条Entry记录的索引值
func (u *unstable) maybeLastIndex() (uint64, bool) {
	//检查切片的长度
	if l := len(u.entries); l != 0 {
		//返回entries中最后一条Entry记录的索引值
		return u.offset + uint64(l) - 1, true
	}
	//如果存在快照数据，则通过其元数据返回索引值
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	//entries和snapshot都是空的，这个unstable其实也就没有任何数据了
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
//尝试获取指定Entry记录的Term值
//根据条件查找指定的Entry记录的位置
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	//指定索引值对应的Entry记录已经不在unstable中
	//可能已经被持久化或是被写入快照
	if i < u.offset {
		//检查是不是快照所包含的最后一条Entry记录
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	//获取unstable中最后一条Entry记录的索引
	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	//指定的索引值超出了unstable已知范围，查找失败
	if i > last {
		return 0, false
	}
	//从entries字段中查找指定的Entry并返回其term值
	return u.entries[i-u.offset].Term, true
}

//清除entries中对应的Entry记录
func (u *unstable) stableTo(i, t uint64) {
	//查找指定Entry记录的term值
	//若查找失败表示对应的entry不在unstable中，直接返回
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	//指定的Entry记录在entries中存在
	if gt == t && i >= u.offset {

		//指定索引值之前的Entry记录都已经完成持久化，则将其之前的全部Entry记录删除
		u.entries = u.entries[i+1-u.offset:]
		//更新offset字段
		u.offset = i + 1
		//随着多次追加日志和截断日志的操作，unstable.entries底层的数组越来越大
		//shrinkEntriesArray()方法会在底层数组长度超过实际占用的两倍时，对底层数组进行缩减
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	//检查entries的长度
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		//重新创建切片
		newEntries := make([]pb.Entry, len(u.entries))
		//复制原有切片中的数据
		copy(newEntries, u.entries)
		//重置entries字段
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}
//向unstable.entries中追加Entry记录
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	//获取第一条待追加的Entry记录的索引值
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		//若待追加的记录与entries中的记录正好连续，则可以直接向entries中追加
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		//直接用待追加的Entry记录替换当前的entries字段，并更新offset
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		//after在offset~last之间，则after~last之间的Entry记录冲突
		//这里会将offset~after之间的记录保留
		//抛弃after之后的记录，然后完成追加操作
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}
//检查边界是否合法并返回offset~after的切片
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
