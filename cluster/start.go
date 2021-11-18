package cluster

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"strings"
)

func StartServe(cluster string, id, kvport int, join bool) {
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(id, strings.Split(cluster, ","), join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, kvport, confChangeC, errorC)
}

func StartChickServe(cluster string, id int, join bool) {
	newChickNode(id, strings.Split(cluster, ","), join)
}

func StartOwlServe(cluster string, id int, join bool) {
	proposeC := make(chan string)
	//defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	//defer close(confChangeC)

	commitC, errorC, _ := newRaftNode(id, strings.Split(cluster, ","), join, nil, proposeC, confChangeC)

	owls := newOwl(proposeC, commitC, errorC)
	owls.live()
}
