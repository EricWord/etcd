// Copyright 2019 The etcd Authors
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

package tracker

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	// the starting index in the buffer
	//buffer数组被当做一个环形数组使用，start字段中记录buffer中第一条MsgApp消息的下标
	start int
	// number of inflights in the buffer
	//当前inflights实例中记录的MsgApp消息个数
	count int

	// the size of the buffer
	//当前inflights实例中能够记录的MsgApp个数的上限
	size int

	// buffer contains the index of the last entry
	// inside one message.
	//用来记录MsgApp消息相关信息的数组，其中记录的是MsgApp消息最后一条Entry记录的索引值
	buffer []uint64
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is room
// for one more message, and consecutive calls to add Add() must provide a
// monotonic sequence of indexes.
//用来记录发送出去的MsgApp消息
func (in *Inflights) Add(inflight uint64) {
	//检查当前buffer数组是否已经被填充满了
	if in.Full() {
		panic("cannot add into a Full inflights")
	}
	//获取新增消息的下标
	next := in.start + in.count
	size := in.size
	//环形队列
	if next >= size {
		next -= size
	}
	//初始化时buffer数组较短，随着使用会不断的扩容(2倍)，但是其扩容上限为size
	if next >= len(in.buffer) {
		in.grow()
	}
	//在next位置记录消息中最后一条entry记录的索引值
	in.buffer[next] = inflight
	//递增count字段
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
//将指定消息及其之前的消息全部清空，释放inflights空间，让后面的消息继续发送
func (in *Inflights) FreeLE(to uint64) {
	//边界检查
	//检查当前inflights是否为空以及参数to是否有效

	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		//查找第一个大于指定索引值的位置
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		//因为是环形队列，如果idx越界，则从0开始继续遍历
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	//i记录了此次释放的消息个数
	in.count -= i
	//从start~idx的所有消息都被释放(注意，是环形队列)
	in.start = idx
	//inflights中全部消息都被清空了，则重置start
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

// FreeFirstOne releases the first inflight. This is a no-op if nothing is
// inflight.
//只释放其中记录的第一条MsgApp消息
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full returns true if no more messages can be sent at the moment.
//判断当前inflights实例是否被填充满了
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
