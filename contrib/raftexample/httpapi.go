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
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"etcd-with-comments/raft/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	//保存用户提交的键值对信息
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//获取请求的URI作为key
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		//读取HTTP请求体
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		//对键值对进行序列化，之后将结果写入proposeC通道
		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		//向用户返回相应的状态码
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		//直接从kvstore中读取指定的键值对数据，并返回给用户
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	//	向集群中新增指定的节点
	case r.Method == "POST":
		//读取请求体，获取新加节点的url
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		//创建ConfChange消息
		cc := raftpb.ConfChange{
			//表示新增节点
			Type: raftpb.ConfChangeAddNode,
			//指定新增节点的id
			NodeID: nodeId,
			//指定新增节点的url
			Context: url,
		}
		//将ConfChange实例写入confChangeC通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		//返回响应的状态码
		w.WriteHeader(http.StatusNoContent)
	//	从集群中删除指定的节点
	case r.Method == "DELETE":
		//解析key得到的待删除的节点id
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			//指定待删除的节点id
			NodeID: nodeId,
		}
		//将ConfChange实例发送到confChangeC通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		//给客户端返回204状态码
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
//监听指定的地址，接收用户发来的http请求
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	//创建http.Server用于接收http请求
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	//启动单独的goroutine来监听Addr指定的地址，当有http请求时，http.Server会创建对应的goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
