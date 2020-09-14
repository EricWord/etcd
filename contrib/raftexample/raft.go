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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"etcd-with-comments/etcdserver/api/rafthttp"
	"etcd-with-comments/etcdserver/api/snap"
	stats "etcd-with-comments/etcdserver/api/v2stats"
	"etcd-with-comments/pkg/fileutil"
	"etcd-with-comments/pkg/types"
	"etcd-with-comments/raft"
	"etcd-with-comments/raft/raftpb"
	"etcd-with-comments/wal"
	"etcd-with-comments/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	//当收到HTTP PUT请求时，httpKVAPI会将请求中的键值对信息通过proposeC通道传递给raftNode实例进行处理
	//PUT请求表示添加键值对数据
	proposeC <-chan string // proposed messages (k,v)
	//POST请求表示集群节点修改的请求
	//当收到POST请求时，httpKVAPI会通过confChangeC通道将修改的节点id传递给raftNode实例进行处理
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	//在创建raftNode实例之后，会返回commitC、errorC、snapshotterReady三个通道
	//raftNode会将etcd-raft模块返回的待应用entry记录(封装在Ready实例中)写入commitC通道
	//kvstore会从commitC通道中读取这些待应用的entry记录并保存其中的键值对信息
	commitC chan<- *string // entries committed to log (k,v)
	//当etcd-raft模块关闭或者是出现异常的时候，会通过errorC通道将该信息通知上层模块
	errorC chan<- error // errors from raft session

	//记录当前节点的id
	id int // client ID for raft session
	//当前集群中所有节点的地址，当前节点会通过该字段中保存的地址向其他节点发送消息
	peers []string // raft peer URLs
	//当前节点是否为后续加入到一个集群的节点
	join bool // node is joining an existing cluster
	//存放WAL日志文件的目录
	waldir string // path to WAL directory
	//存放快照文件的目录
	snapdir string // path to snapshot directory
	//用于获取快照数据的函数
	getSnapshot func() ([]byte, error)
	//当回放WAL日志结束之后，会使用该字段记录最后一条entry记录的索引值
	lastIndex uint64 // index of log at start

	//记录当前集群的状态
	confState raftpb.ConfState
	//保存当前快照的相关元数据，即快照所包含的最后一条entry记录的索引值
	snapshotIndex uint64
	//保存上层模块已应用的位置，即已应用的最后一条entry记录的索引值
	appliedIndex uint64

	// raft backing for the commit/error channel
	//实现了Node接口，并将etcd-raft模块的API接口暴露给了上层模块
	node        raft.Node
	raftStorage *raft.MemoryStorage
	//负责WAL日志的管理
	wal *wal.WAL

	//负责管理快照数据
	//etcd-raft模块并没有完成快照数据的管理，而是将其独立成一个单独的模块
	snapshotter *snap.Snapshotter
	//主要用于初始化的过程中监听snapshotter实例是否创建完成，snapshotter负责管理etcd-raft模块产生的快照数据
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	//两次生成快照之间间隔的entry记录数
	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
//raftNode的初始化、
//启动一个独立的后台goroutine完成回放WAL日志、启动网络组件等初始化操作
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {
	//commitC和errorC这两个通道用于返回给上层模块使用
	commitC := make(chan *string)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		//初始化存放WAL日志的目录
		waldir: fmt.Sprintf("raftexample-%d", id),
		//初始化存放SnapShot文件的目录
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		//创建下面四个通道
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
		//	其余字段在WAL日志回放完成之后才会初始化
	}
	//单独启动一个goroutine执行startRaft()方法，在该方法中完成剩余初始化操作
	go rc.startRaft()
	//将通道返回给上层应用
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	//根据快照的元数据，创建walpb.Snapshot实例
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	//WAL会将上述快照的元数据信息封装成一条日志记录下来
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	//根据快照的元数据信息，释放一些无用的WAL日志文件的句柄
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

//对Ready实例中携带的待应用记录进行过滤
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	//检查ents长度
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	//检查firstIdx是否合法
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	//过滤调已经被应用过的Entry记录
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			//如果Entry记录的Data为空，则直接忽略该条Entry记录
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			//将数据写入commitC通道，kvstore会从其中读取并记录相应的kv值
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			//将ConfChange实例传入底层的etcd-raft组件
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	//读取快照文件
	snapshot := rc.loadSnapshot()
	//根据读取到的snapshot实例的元数据创建WAL实例
	w := rc.openWAL(snapshot)
	//读取快照数据之后的全部WAL日志数据，并获取状态信息
	_, st, ents, err := w.ReadAll()
	//异常检查
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	//将快照数据加载到MemStorage中
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	//将读取WAL日志之后得到的HardState加载到MemoryStorage中
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	//将读取WAL日志得到的Entry记录加载到MemoryStorage中
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	//检查snapdir字段指定的目录是否存在，该目录用于存放定期生成的快照数据
	//若snapdir目录不存在，则进行创建，若创建失败，则输出异常并终止程序
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	//1、创建snapshotter实例，并将该snapshotter通过snapshotterReady通道返回给上层应用
	//snapshotter实例提供了读写快照文件的功能
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	//2、创建WAL实例，然后加载快照并回放WAL日志
	//检查waldir目录下是否存在旧的WAL日志文件
	oldwal := wal.Exist(rc.waldir)
	//先加载快照数据，然后重放WAL日志文件
	rc.wal = rc.replayWAL()

	//3、创建raft.Config实例
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:           uint64(rc.id),
		ElectionTick: 10,
		//心跳计时器超时时间
		HeartbeatTick: 1,
		//持久化存储
		Storage: rc.raftStorage,
		//每条消息的最大长度
		MaxSizePerMsg: 1024 * 1024,
		//已经发送但是未收到响应的消息个数上限
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	//4、初始化底层的etcd-raft模块，这里会根据WAL日志的回放情况，
	//判断当前节点是首次启动还是重新启动
	if oldwal {
		//重启节点
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		//初次启动节点
		rc.node = raft.StartNode(c, startPeers)
	}

	//5、创建Transport实例并启动，它负责raft节点之间的通信的网络服务
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	//启动网络服务相关的组件
	rc.transport.Start()
	//6、建立与集群中其他几个节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	//7、启动一个goroutine，会监听当前节点与集群中其他节点之间的网路连接
	go rc.serveRaft()
	//8、启动后台goroutine处理上层应用与底层etcd-raft模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	//使用commitC通道通知上层应用加载新生成的快照数据
	rc.commitC <- nil // trigger kvstore to load snapshot
	//记录新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000
//触发快照生成操作
func (rc *raftNode) maybeTriggerSnapshot() {
	//检查处理的记录数是否足够。如果不足，直接返回
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

//负责监听当前节点的地址，完成与其他节点的通信6
func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
