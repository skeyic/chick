package owluster

import (
	"errors"
	"github.com/golang/glog"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// Errors ..
var (
	errRPCConnectFailed  = errors.New("failed to connect through RPC")
	errLeaderNotElected  = errors.New("leader not elected")
	errClusterNotHealthy = errors.New("cluster not healthy")
)

// node cluster node
type node struct {
	//connect  bool
	Address           string
	IsMasterCandidate bool
}

// State int
type State int

// Follower, PreCandidate, Candidate, Leader
const (
	Beginner State = iota + 1
	Follower
	PreCandidate
	Candidate
	Leader
)

type OwlData interface {
	Zip() []byte         // struct to bytes
	Unzip([]byte)        // bytes to struct
	Do(entry *LogEntry)  // do log
	Version() (int, int) // committed log index
	Report()
}

type LogEntry struct {
	LogTerm  int
	LogIndex int

	Msg  string
	Done bool
}

type food struct {
	msg    string
	errorC chan error
}

func newFood(msg string) *food {
	return &food{
		msg:    msg,
		errorC: make(chan error, 1),
	}
}

type Beak struct {
	proposeC chan *food
}

func NewBeak() *Beak {
	return &Beak{proposeC: make(chan *food, 100)}
}

func (b *Beak) Eat(msg string) {
	b.proposeC <- newFood(msg)
}

func (b *Beak) Chew(msg string) error {
	var (
		f = newFood(msg)
	)
	b.proposeC <- f
	for {
		select {
		case err := <-f.errorC:
			return err
		}
	}
}

type Maw struct {
	lock       *sync.RWMutex
	logs       []*LogEntry
	startIndex int
	lastIndex  int
	lastTerm   int
}

func newMaw() *Maw {
	return &Maw{
		lock:       &sync.RWMutex{},
		startIndex: -1,
		lastIndex:  -1,
		lastTerm:   -1,
	}
}

func (m *Maw) len() int {
	return len(m.logs)
}

func (m *Maw) getLastIndex() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.lastIndex
}

func (m *Maw) getLastTerm() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.lastTerm
}

func (m *Maw) getLogsSince(startIdx int) []*LogEntry {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var realIdx int
	for idx, log := range m.logs {
		if log.LogIndex == startIdx {
			realIdx = idx
			break
		}
	}
	return m.logs[realIdx+1:]
}

func (m *Maw) emptyByData(data OwlData) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logs = nil
	m.lastTerm, m.lastIndex = data.Version()
}

func (m *Maw) appendLog(msg string, term int) *LogEntry {
	m.maybeSnapshot()
	m.lock.Lock()
	defer m.lock.Unlock()

	m.lastTerm = term
	m.lastIndex++
	newLog := &LogEntry{
		LogTerm:  term,
		LogIndex: m.lastIndex,
		Msg:      msg,
	}
	m.logs = append(m.logs, newLog)
	return newLog
}

func (m *Maw) addLog(msg string, term, index int) *LogEntry {
	m.maybeSnapshot()
	m.lock.Lock()
	defer m.lock.Unlock()

	m.lastIndex = index
	m.lastTerm = term
	newLog := &LogEntry{
		LogTerm:  term,
		LogIndex: index,
		Msg:      msg,
	}
	m.logs = append(m.logs, newLog)
	return newLog
}

func (m *Maw) report() {
	m.lock.RLock()
	defer m.lock.RUnlock()
	glog.V(4).Infof("MAW REPORT: LATEST IDX: %d, TERM: %d", m.lastIndex, m.lastTerm)
	for idx, value := range m.logs {
		glog.V(4).Infof("IDX: %d, VALUE: %+v", idx, value)
	}
}

const defaultSnapshotSize = 10

func (m *Maw) maybeSnapshot() {
	m.lock.Lock()
	if len(m.logs) > defaultSnapshotSize {
		if m.logs[defaultSnapshotSize].Done {
			m.logs = m.logs[defaultSnapshotSize:]
		}
	}
	m.lock.Unlock()
}

const healthCheckGap = 3

type healthChecker struct {
	lock               *sync.RWMutex
	idx                int
	lastHealthyIdx     int
	masterCheckedCount int
	nodes              map[string]int
	masterNum          int
}

func newHealthChecker() *healthChecker {
	return &healthChecker{
		lock:           &sync.RWMutex{},
		idx:            -1,
		lastHealthyIdx: -1,
		nodes:          make(map[string]int),
	}
}

func (h *healthChecker) start() int {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.idx++
	h.masterCheckedCount = 1 // we have myself
	return h.idx
}

func (h *healthChecker) add(address string, idx int) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.nodes[address] = idx
}

func (h *healthChecker) addMasterCandidate(address string, idx int) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.nodes[address] = idx
	if idx == h.idx {
		h.masterCheckedCount++
		if h.masterCheckedCount >= h.masterNum/2+1 {
			h.lastHealthyIdx = h.idx
		}
	}
}

func (h *healthChecker) isNodeHealthy(node string) bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	nodeIdx := h.nodes[node]
	//glog.V(4).Infof("IDX: %d, NODE %s LAST: %d, HEALTHY: %v", h.idx, node, nodeIdx, h.idx-nodeIdx < healthCheckGap)
	//glog.V(4).Infof("IDXs: %+v", h.nodes)
	return h.idx-nodeIdx < healthCheckGap
}

func (h *healthChecker) isHealthy() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	glog.V(4).Infof("IDX: %d, LAST: %d, HEALTHY: %v", h.idx, h.lastHealthyIdx, h.idx-h.lastHealthyIdx < healthCheckGap)
	glog.V(4).Infof("IDXs: %+v", h.nodes)
	return h.idx-h.lastHealthyIdx < healthCheckGap
}

func (h *healthChecker) getHealthyNodesNum() (num int) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, nodeIdx := range h.nodes {
		if h.idx-nodeIdx < healthCheckGap {
			num++
		}
	}
	// Add myself
	num++
	return
}

// Owl node
type Owl struct {
	// my Address
	me string

	// process channel
	proposeC chan *food
	beak     *Beak

	// leader id
	leaderAddress string

	// all nodes include me
	nodes map[string]*node

	// able to become master
	isMasterCandidate  bool
	masterCandidateNum int

	// rpc client to other nodes
	clientLock *sync.RWMutex
	clients    map[string]*rpc.Client

	// current state
	state State

	// current cluster health, has quorum or not
	healthChecker *healthChecker

	// current term
	currentTerm int

	// vote for who in this term, "" for nobody
	voteLock *sync.RWMutex
	votedFor string

	// get voted masterCheckedCount in this term
	voteCount    int
	preVoteCount int

	// heartbeat channel
	heartbeatCount int
	heartbeatC     chan bool

	// to leader channel
	toLeaderC chan bool

	// to candidate channel
	toCandidateC chan bool

	// join accept channel
	joinAcceptedC chan bool

	//// last committed log index
	//committedLogIndex int

	// last applied(processed) log index
	appliedLogIndex int

	// the main data which all the logEntries will be committed into
	data OwlData

	// the maw
	maw *Maw

	// record the current log index from each node
	clusterLock         *sync.RWMutex
	clusterNodeStatsMap map[string]*nodeStats
}

type nodeStats struct {
	node           string
	logIndex       int
	term           int
	dataProcessing bool
	lock           *sync.RWMutex
}

func newNodeStats(node string) *nodeStats {
	return &nodeStats{
		node:     node,
		logIndex: -1,
		term:     -1,
		lock:     &sync.RWMutex{},
	}
}

func (n *nodeStats) TryDataProcessing() bool {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.dataProcessing {
		return false
	}
	n.dataProcessing = true
	glog.V(4).Info("SET DATA PROCESSING")
	return true
}

func (n *nodeStats) FinishDataProcessing() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.dataProcessing = false
	glog.V(4).Info("BACK DATA PROCESSING")
}

func (n *nodeStats) GetStats() (int, int) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.term, n.logIndex
}

func (n *nodeStats) SetStats(term, index int) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.term = term
	n.logIndex = index
}

func NewOwl(address, cluster string, beak *Beak, data OwlData) *Owl {
	owl := &Owl{
		me:                  address,
		beak:                beak,
		proposeC:            beak.proposeC,
		maw:                 newMaw(),
		data:                data,
		nodes:               make(map[string]*node),
		clients:             make(map[string]*rpc.Client),
		clientLock:          &sync.RWMutex{},
		voteLock:            &sync.RWMutex{},
		clusterLock:         &sync.RWMutex{},
		clusterNodeStatsMap: make(map[string]*nodeStats),
		healthChecker:       newHealthChecker(),
		heartbeatC:          make(chan bool),
		toCandidateC:        make(chan bool),
		toLeaderC:           make(chan bool),
		joinAcceptedC:       make(chan bool),
	}

	clusters := strings.Split(cluster, ",")
	for _, v := range clusters {
		if v == address {
			owl.isMasterCandidate = true
		}
		owl.nodes[v] = &node{Address: v, IsMasterCandidate: true}
		owl.healthChecker.nodes[v] = -1
		owl.masterCandidateNum++
	}

	owl.rpc(address)

	owl.start()

	return owl
}

func (o *Owl) rpc(port string) {
	err := rpc.Register(o)
	if err != nil {
		glog.Fatalf("failed to register rpc, error: %v", err)
	}
	rpc.HandleHTTP()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		glog.Fatalf("failed to listen to %s, error: %v", port, err)
	}
	glog.V(4).Infof("start to listen to %s", port)

	go http.Serve(lis, nil)
}

func (o *Owl) IncreaseTerm() {
	o.currentTerm++
	o.updateMyStatus()
}

func (o *Owl) getLastLogIndex() int {
	return o.maw.getLastIndex()
}

func (o *Owl) getLastTerm() int {
	return o.currentTerm
}

func (o *Owl) getClient(address string) (*rpc.Client, error) {
	o.clientLock.RLock()
	client, ok := o.clients[address]
	o.clientLock.RUnlock()
	if ok && o.sendHello(client) == nil {
		return client, nil
	}

	client, err := rpc.DialHTTP("tcp", o.nodes[address].Address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", o.nodes[address].Address, err)
		return nil, errRPCConnectFailed
	}
	o.clientLock.Lock()
	o.clients[address] = client
	o.clientLock.Unlock()

	return client, err
}

func (o *Owl) newClient(address string) (*rpc.Client, error) {
	o.clientLock.Lock()
	defer o.clientLock.Unlock()
	client, err := rpc.DialHTTP("tcp", o.nodes[address].Address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", o.nodes[address].Address, err)
		return nil, errRPCConnectFailed
	}
	o.clients[address] = client

	return client, err
}

func (o *Owl) getVoteFor() string {
	o.voteLock.RLock()
	glog.V(4).Infof("GET VOTE FOR: >>%s<<", o.votedFor)
	defer o.voteLock.RUnlock()
	return o.votedFor
}

func (o *Owl) setVoteFor(target string) {
	glog.V(4).Infof("SET VOTE FOR TO >>%s<<", target)
	o.voteLock.Lock()
	o.votedFor = target
	o.voteLock.Unlock()
}

func (o *Owl) getNodes() []*node {
	o.clusterLock.RLock()
	defer o.clusterLock.RUnlock()
	var nodes []*node
	for _, n := range o.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (o *Owl) updateNodes(nodes []*node) {
	o.clusterLock.Lock()
	defer o.clusterLock.Unlock()
	for _, n := range nodes {
		o.nodes[n.Address] = n
	}
}

func (o *Owl) getNodesStatus() []*nodeStats {
	o.clusterLock.RLock()
	defer o.clusterLock.RUnlock()
	var nodes []*nodeStats
	for _, n := range o.clusterNodeStatsMap {
		nodes = append(nodes, n)
	}
	return nodes
}

func (o *Owl) updateMyStatus() {
	o.clusterLock.Lock()
	status := o.clusterNodeStatsMap[o.me]
	status.lock.Lock()
	status.logIndex = o.getLastLogIndex()
	status.term = o.currentTerm
	status.lock.Unlock()
	o.clusterLock.Unlock()
}

func (o *Owl) isMe(n *node) bool {
	return o.me == n.Address
}

func (o *Owl) isHealthy() bool {
	return o.healthChecker.isHealthy()
}

func (o *Owl) start() {
	if o.isMasterCandidate {
		o.state = Follower
	} else {
		o.state = Beginner
	}
	o.currentTerm = 0
	o.leaderAddress = ""
	o.votedFor = ""
	o.heartbeatC = make(chan bool)
	o.toLeaderC = make(chan bool)

	// state change and handle RPC
	go o.step()
	go o.serveChannels()
	go o.debug()
}

func (o *Owl) step() {
	rand.Seed(time.Now().UnixNano())

	for {
		switch o.state {
		case Beginner:
			go o.broadcastRequestJoin()
			select {
			case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
				glog.V(4).Infof("node-%s requests join timeout", o.me)
			case <-o.joinAcceptedC:
				o.state = Follower
			}

		case Follower:
			glog.V(10).Infof("node-%s is a follower", o.me)
			select {
			case <-o.heartbeatC:
				glog.V(10).Infof("node-%s receives heartbeat", o.me)

			case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
				if o.isMasterCandidate {
					glog.V(4).Infof("node-%s receives heartbeat timeout, Follower => PreCandidate", o.me)
					o.state = PreCandidate
				}
			}

		case PreCandidate:
			glog.V(4).Infof("node-%s is a pre candidate, pre vote for myself, my term: %d", o.me, o.currentTerm)
			o.leaderAddress = ""
			o.setVoteFor("")
			o.preVoteCount = 1
			go o.broadcastRequestVote(true)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("node-%s requests pre vote timeout, PreCandidate => Follower", o.me)
				o.state = Follower
			case <-o.toCandidateC:
				glog.V(4).Infof("node-%s wins the pre vote, PreCandidate => Candidate", o.me)
				o.state = Candidate
			}

		case Candidate:
			o.IncreaseTerm()
			o.setVoteFor(o.me)
			o.voteCount = 1
			glog.V(4).Infof("node-%s is a candidate, vote for myself, my term: %d", o.me, o.currentTerm)

			go o.broadcastRequestVote(false)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("node-%s requests vote timeout, Candidate => Follower", o.me)
				o.state = PreCandidate
			case <-o.toLeaderC:
				glog.V(4).Infof("node-%s wins the vote, Candidate => Leader", o.me)

				o.clusterLock.Lock()
				for _, i := range o.nodes {
					o.clusterNodeStatsMap[i.Address] = newNodeStats(i.Address)
				}
				o.clusterLock.Unlock()

				o.leaderAddress = o.me
				o.updateMyStatus()
				o.state = Leader
			}

		case Leader:
			glog.V(10).Infof("node-%s is a leader, send heartbeat", o.me)
			o.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (o *Owl) serveChannels() {
	for {
		select {
		case food := <-o.beak.proposeC:
			//glog.V(4).Infof("ROLE: %d, FOOD: %+v", o.state, food)
			switch o.state {
			case Follower:
				food.errorC <- o.forwardDataToLeader(food.msg)
			case PreCandidate, Candidate:
				food.errorC <- errLeaderNotElected
			case Leader:
				food.errorC <- o.processData(food.msg)
			}
		}
	}
}

func (o *Owl) debug() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			o.maw.report()
			o.data.Report()

			glog.V(4).Infof("NODES REPORT: TERM: %d", o.currentTerm)
			o.clusterLock.RLock()
			for address := range o.nodes {
				glog.V(4).Infof("NODE: %s", address)
			}
			o.clusterLock.RUnlock()
		}
	}
}

func (o *Owl) processData(msg string) error {
	if !o.isHealthy() {
		return errClusterNotHealthy
	}
	o.data.Do(
		o.maw.appendLog(msg, o.currentTerm),
	)
	o.updateMyStatus()
	return nil
}

func (o *Owl) forwardDataToLeader(msg string) error {
	return o.sendDataToLeader(msg)
}

type VoteArgs struct {
	CurrentTerm       int
	CommittedLogIndex int
	CandidateID       string
	PreVote           bool
}

// VoteReason int
type VoteReason int

// MyTermHigher, MyCommittedLogIndexHigher, AlreadyVoted
const (
	MyTermHigher VoteReason = iota + 1
	MyCommittedLogIndexHigher
	AlreadyVoted
	NotMasterCandidate
)

type VoteReply struct {
	Me                string
	CurrentTerm       int
	CommittedLogIndex int
	VoteGranted       bool
	Reason            VoteReason
}

func (o *Owl) broadcastRequestVote(preVote bool) {
	var (
		args = VoteArgs{
			CurrentTerm:       o.currentTerm,
			CommittedLogIndex: o.getLastLogIndex(),
			CandidateID:       o.me,
			PreVote:           preVote,
		}
	)

	if preVote {
		args.CurrentTerm++
	}

	for _, i := range o.nodes {
		if i.IsMasterCandidate && !o.isMe(i) {
			go func(i *node) {
				var (
					reply = new(VoteReply)
				)
				o.sendRequestVote(i, args, reply)
			}(i)
		}
	}
}

func (o *Owl) sendRequestVote(server *node, args VoteArgs, reply *VoteReply) {
	var (
		serverAddress = server.Address
	)
	client, err := o.getClient(serverAddress)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %s with error %v", serverAddress, err)
		return
	}

	glog.V(4).Infof("sendRequestVote %s, ARGS: %+v", serverAddress, args)
	err = client.Call("Owl.RequestVote", args, reply)
	if err != nil {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.RequestVote", err)
		return
	}
	glog.V(4).Infof("REPLY: %+v, MY TERM: %d", reply, o.currentTerm)

	if args.PreVote {
		if reply.VoteGranted {
			o.preVoteCount++

			if o.preVoteCount >= o.masterCandidateNum/2+1 {
				o.toCandidateC <- true
			}
		}
	} else {
		if reply.CurrentTerm > o.currentTerm {
			o.currentTerm = reply.CurrentTerm
			o.state = Follower
			o.votedFor = ""
			return
		}

		if reply.VoteGranted {
			o.voteCount++

			if o.voteCount >= o.masterCandidateNum/2+1 {
				o.toLeaderC <- true
			}
		}
	}
}

func (o *Owl) RequestVote(args VoteArgs, reply *VoteReply) error {
	reply.Me = o.me

	if !o.isMasterCandidate {
		reply.Reason = NotMasterCandidate
		reply.VoteGranted = false
		return nil
	}

	reply.CurrentTerm = o.currentTerm
	reply.CommittedLogIndex = o.getLastLogIndex()

	// My term is higher, reject
	if args.CurrentTerm < o.currentTerm {
		reply.VoteGranted = false
		reply.Reason = MyTermHigher
		return nil
	}

	// My last log index is higher, reject
	if args.CommittedLogIndex < o.getLastLogIndex() {
		reply.VoteGranted = false
		reply.Reason = MyCommittedLogIndexHigher
		return nil
	}

	if o.getVoteFor() == "" {
		if !args.PreVote {
			o.currentTerm = args.CurrentTerm
			o.setVoteFor(args.CandidateID)
		}

		reply.VoteGranted = true
		return nil
	}

	reply.Reason = AlreadyVoted
	reply.VoteGranted = false
	return nil
}

type HeartbeatArgs struct {
	Idx  int
	Term int
	Me   string

	CurrentLogTerm int
	Logs           []*LogEntry
	Data           []byte
	PrevLogIndex   int
	PrevLogTerm    int

	Nodes []*node
}

func (a *HeartbeatArgs) WithData() bool {
	return len(a.Logs) != 0 || len(a.Data) != 0 || len(a.Nodes) != 0
}

type HeartbeatReply struct {
	Me              string
	Idx             int
	Term            int
	Success         bool
	CurrentLogIndex int
}

func (o *Owl) broadcastHeartbeat() {
	idx := o.healthChecker.start()

	for _, i := range o.nodes {
		if o.isMe(i) {
			continue
		}

		var (
			args = HeartbeatArgs{
				Idx:            idx,
				Term:           o.currentTerm,
				Me:             o.me,
				CurrentLogTerm: o.getLastTerm(),
			}
		)

		o.clusterLock.RLock()
		prevNodeStats := o.clusterNodeStatsMap[i.Address]
		o.clusterLock.RUnlock()

		var (
			tryDataProcessing      bool
			isHealthy              = o.healthChecker.isNodeHealthy(i.Address)
			prevTerm, prevLogIndex = prevNodeStats.GetStats()
		)

		//glog.V(4).Infof("currentTerm: %d, prevTerm: %d, IS HEALTHY: %v, lastLogIndex: %d, prevLogIndex: %d",
		//	o.currentTerm, prevTerm, isHealthy, o.getLastLogIndex(), prevLogIndex)

		if o.currentTerm > prevTerm && isHealthy {
			if prevNodeStats.TryDataProcessing() {
				tryDataProcessing = true
				args.Nodes = o.getNodes()
				glog.V(4).Infof("TO NODE: %s, IDX: %d, PT: %d, SEND NODES %v", i.Address, idx, prevTerm, args.Nodes)
			}
		}

		if o.getLastLogIndex() > prevLogIndex && isHealthy {
			if tryDataProcessing || prevNodeStats.TryDataProcessing() {
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = o.maw.getLastTerm()
				if prevLogIndex == -1 {
					args.Data = o.data.Zip()
					glog.V(4).Infof("TO NODE: %s, IDX: %d, PI: %d, SEND DATA %s", i.Address, idx, prevLogIndex, args.Data)
				} else {
					args.Logs = o.maw.getLogsSince(prevLogIndex)
					if len(args.Logs) == 0 {
						args.Data = o.data.Zip()
						glog.V(4).Infof("DATA INSTEAD LOGS, TO NODE: %s, IDX: %d, PI: %d, SEND DATA %s", i.Address, idx, prevLogIndex, args.Data)
					} else {
						glog.V(4).Infof("TO NODE: %s, IDX: %d, PI: %d, SEND LOGS %v", i.Address, idx, prevLogIndex, args.Logs)
					}
				}
			}
		}

		go func(i *node, args HeartbeatArgs) {
			var (
				reply = new(HeartbeatReply)
			)
			o.sendHeartbeat(i, args, reply)
		}(i, args)
	}
}

func (o *Owl) sendHeartbeat(server *node, args HeartbeatArgs, reply *HeartbeatReply) {
	var (
		serverAddress = server.Address
	)

	defer func() {
		if args.WithData() {
			o.clusterLock.Lock()
			o.clusterNodeStatsMap[serverAddress].FinishDataProcessing()
			o.clusterLock.Unlock()
		}
	}()

	client, err := o.getClient(serverAddress)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %s with error %v", serverAddress, err)
		return
	}

	err = client.Call("Owl.Heartbeat", args, &reply)
	if err != nil {
		glog.V(4).Infof("failed to call %s with error %v", "Owl.Heartbeat", err)
		return
	}

	if args.WithData() {
		glog.V(10).Infof("sendHeartbeat to %s, ARGS: %+v", serverAddress, args)
	}

	if reply.Success {
		if reply.CurrentLogIndex >= -1 {
			o.clusterLock.Lock()
			o.clusterNodeStatsMap[serverAddress].SetStats(reply.Term, reply.CurrentLogIndex)
			o.clusterLock.Unlock()
		}
		if server.IsMasterCandidate {
			o.healthChecker.addMasterCandidate(reply.Me, reply.Idx)
		} else {
			o.healthChecker.add(reply.Me, reply.Idx)
		}
	} else {
		glog.V(4).Infof("sendHeartbeat REPLY from %s: ARGS: %+v, REPLY: %+v", serverAddress, args, reply)
		// TWO CASES:
		// 1. its term is higher than mine, we need to become the follower
		// 2. the prev log index is not correct, do nothing, next time heartbeat we will send using the correct index
		if server.IsMasterCandidate {
			if reply.Term > o.currentTerm {
				o.currentTerm = reply.Term
				o.state = Follower
				o.votedFor = ""
			}
		}
	}
	//if args.WithData() {
	//	glog.V(4).Infof("ARGS: %+v, REPLY: %+v", args, reply)
	//}
}

func (o *Owl) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	reply.Me = o.me
	reply.Idx = args.Idx

	// My term is higher
	if args.Term < o.currentTerm {
		reply.Term = o.currentTerm
		return nil
	}

	// Leader's term is higher
	if args.Term > o.currentTerm {
		if len(args.Nodes) > 0 {
			o.updateNodes(args.Nodes)
			o.currentTerm = args.Term
		}
	}

	o.leaderAddress = args.Me
	o.heartbeatC <- true

	// Empty heartbeat, just tell leader our current log term
	if len(args.Logs) == 0 && len(args.Data) == 0 {
		reply.Success = true
		reply.Term = o.currentTerm
		reply.CurrentLogIndex = o.getLastLogIndex()
		return nil
	}

	// pre log index is correct, let's process the new maw and return the latest log term to leader
	// otherwise the pre log index is not match, do nothing but tell leader our current log term
	if args.PrevLogIndex == o.getLastLogIndex() {
		if len(args.Data) != 0 {
			o.data.Unzip(args.Data)
			o.maw.emptyByData(o.data)
		} else {
			for _, log := range args.Logs {
				o.data.Do(
					o.maw.addLog(log.Msg, log.LogTerm, log.LogIndex),
				)
			}
		}
	}

	reply.Success = true
	reply.Term = o.currentTerm
	reply.CurrentLogIndex = o.getLastLogIndex()

	return nil
}

type DataArgs struct {
	Data string
}

type DataReply struct {
	Success bool
}

func (o *Owl) clientHealthCheck() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			for id := range o.nodes {
				var (
					client *rpc.Client
					err    error
				)
				client, err = o.getClient(id)
				if err != nil || o.sendHello(client) != nil {
					client, err = o.newClient(id)
				}
			}
		}
	}
}

func (o *Owl) sendHello(client *rpc.Client) error {
	var (
		args = &DataArgs{
			Data: "hello",
		}
		reply = new(DataReply)
	)
	err := client.Call("Owl.Hello", args, reply)
	if err != nil || !reply.Success {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.Hello", err)
		return errRPCConnectFailed
	}
	return nil
}

func (o *Owl) Hello(args DataArgs, reply *DataReply) error {
	reply.Success = true
	return nil
}

func (o *Owl) sendDataToLeader(msg string) error {
	var (
		args = &DataArgs{
			Data: msg,
		}
		reply    = new(DataReply)
		leaderID = o.leaderAddress
	)

	if leaderID == "" {
		return errLeaderNotElected
	}

	client, err := o.getClient(leaderID)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %s with error %v", leaderID, err)
		return errRPCConnectFailed
	}

	err = client.Call("Owl.ReceiveData", args, reply)
	glog.V(4).Infof("ARGS: %+v, REPLY: %+v", args, reply)
	if err != nil || !reply.Success {
		glog.Errorf("failed to call %s with error %v", "Owl.ReceiveData", err)
		return errRPCConnectFailed
	}
	return nil
}

func (o *Owl) ReceiveData(args DataArgs, reply *DataReply) error {
	if o.processData(args.Data) == nil {
		reply.Success = true
	}
	return nil
}

type JoinArgs struct {
	Address string
}

type JoinReply struct {
	Me           string
	JoinAccepted bool
}

func (o *Owl) broadcastRequestJoin() {
	var (
		args = JoinArgs{
			Address: o.me,
		}
	)

	for _, i := range o.nodes {
		if i.IsMasterCandidate && !o.isMe(i) {
			go func(i *node) {
				var (
					reply = new(JoinReply)
				)
				o.sendRequestJoin(i, args, reply)
			}(i)
		}
	}
}

func (o *Owl) sendRequestJoin(server *node, args JoinArgs, reply *JoinReply) {
	var (
		serverAddress = server.Address
	)
	client, err := o.getClient(serverAddress)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %s with error %v", serverAddress, err)
		return
	}

	glog.V(4).Infof("sendRequestJoin %s, ARGS: %+v", serverAddress, args)
	err = client.Call("Owl.RequestJoin", args, reply)
	if err != nil {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.RequestJoin", err)
		return
	}
	glog.V(4).Infof("REPLY: %+v, MY TERM: %d", reply, o.currentTerm)

	if reply.JoinAccepted {
		o.joinAcceptedC <- true
	}
}

func (o *Owl) RequestJoin(args JoinArgs, reply *JoinReply) error {
	reply.Me = o.me

	if o.state == Leader {
		o.clusterLock.Lock()
		o.nodes[args.Address] = &node{
			Address: args.Address,
		}
		o.clusterNodeStatsMap[args.Address] = newNodeStats(args.Address)
		o.clusterLock.Unlock()
		o.IncreaseTerm()

		reply.JoinAccepted = true
	}

	return nil
}
