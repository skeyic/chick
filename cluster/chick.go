package cluster

import (
	"context"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

type chickNode struct {
	id    int      // client ID for raft session
	peers []string // raft peer URLs
	join  bool     // node is joining an existing cluster

	confState raftpb.ConfState

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage

	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

func newChickNode(id int, peers []string, join bool) *chickNode {
	rc := &chickNode{
		id:    id,
		peers: peers,
		join:  join,

		confState: raftpb.ConfState{},
		node:      nil,
		transport: nil,
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),

		logger: zap.NewExample(),
	}

	go rc.startRaft()

	return rc
}

func (rc *chickNode) startRaft() {
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{
			ID: uint64(i + 1),
		}
	}

	rc.raftStorage = raft.NewMemoryStorage()

	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxUncommittedEntriesSize: 1 << 30,
		MaxInflightMsgs:           256,
	}

	if rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()

	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
}

func (rc *chickNode) serveRaft() {
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

func (rc *chickNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *chickNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *chickNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *chickNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
