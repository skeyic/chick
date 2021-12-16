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
	connect bool
	address string
}

// State int
type State int

// Follower, PreCandidate, Candidate, Leader
const (
	Follower State = iota + 1
	PreCandidate
	Candidate
	Leader
)

type OwlData interface {
	zip() []byte        // struct to bytes
	do(entry *LogEntry) // follow the log entry
	version() int       // committed log index
}

type LogEntry struct {
	// current term
	LogTerm  int
	LogIndex int

	Msg string
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
	lock      *sync.RWMutex
	logs      []*LogEntry
	lastIndex int
	lastTerm  int
}

func newMaw() *Maw {
	return &Maw{
		lock:      &sync.RWMutex{},
		lastIndex: -1,
		lastTerm:  -1,
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

func (m *Maw) getLogsSince(start int) []*LogEntry {
	m.lock.RLock()
	defer m.lock.RUnlock()
	//if start == -1 {
	//	return m.logs
	//}
	return m.logs[start+1:]
}

func (m *Maw) addLog(term int, msg string) {
	m.lock.Lock()
	m.logs = append(m.logs,
		&LogEntry{
			LogTerm:  term,
			LogIndex: len(m.logs),
			Msg:      msg,
		},
	)
	m.lastIndex++
	m.lastTerm = term
	m.lock.Unlock()
}

func (m *Maw) addLogs(logs []*LogEntry) {
	m.lock.Lock()
	m.logs = append(m.logs,
		logs...,
	)
	m.lastTerm = logs[len(logs)-1].LogTerm
	m.lastIndex = len(m.logs) - 1
	m.lock.Unlock()
}

func (m *Maw) report() {
	m.lock.RLock()
	defer m.lock.RUnlock()
	glog.V(4).Infof("==================REPORT==================")
	for idx, value := range m.logs {
		glog.V(4).Infof("IDX: %d, VALUE: %+v", idx, value)
	}
}

func (m *Maw) maybeSnapshot() {
	// Remove committed maw from the maw if there are already too many there
}

const healthCheckGap = 3

type healthChecker struct {
	lock    *sync.RWMutex
	idx     int
	lastIdx int
	count   int
	nodes   map[string]int
}

func newHealthChecker() *healthChecker {
	return &healthChecker{
		lock:    &sync.RWMutex{},
		idx:     -1,
		lastIdx: -1,
		nodes:   make(map[string]int),
	}
}

func (h *healthChecker) start() int {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.idx++
	h.count = 1 // we have myself
	return h.idx
}

func (h *healthChecker) Add(address string, idx int) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.nodes[address] = idx
	if idx == h.idx {
		h.count++
		if h.count >= len(h.nodes)/2+1 {
			h.lastIdx = h.idx
		}
	}
}

func (h *healthChecker) isHealthy() bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	glog.V(4).Infof("IDX: %d, LAST: %d, HEALTHY: %v", h.idx, h.lastIdx, h.idx-h.lastIdx < healthCheckGap)
	glog.V(4).Infof("IDXs: %+v", h.nodes)
	return h.idx-h.lastIdx < healthCheckGap
}

// Owl node
type Owl struct {
	// my id
	me      string
	address string

	// process channel
	proposeC chan *food
	beak     *Beak

	// leader id
	leaderAddress string

	// other nodes except me
	nodes map[string]*node

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

	// get voted count in this term
	voteCount    int
	preVoteCount int

	// heartbeat channel
	heartbeatCount int
	heartbeatC     chan bool

	// to leader channel
	toLeaderC chan bool

	// to candidate channel
	toCandidateC chan bool

	// last committed log index
	committedLogIndex int

	// last applied(processed) log index
	appliedLogIndex int

	// the main data which all the logEntries will be committed into
	data OwlData

	// the maw
	maw *Maw

	// record the current log index from each node
	clusterLock        *sync.RWMutex
	clusterLogIndexMap map[string]int
}

func NewOwl(address, cluster string, beak *Beak) *Owl {
	owl := &Owl{
		me:                 address,
		beak:               beak,
		proposeC:           beak.proposeC,
		maw:                newMaw(),
		nodes:              make(map[string]*node),
		clients:            make(map[string]*rpc.Client),
		clientLock:         &sync.RWMutex{},
		voteLock:           &sync.RWMutex{},
		clusterLock:        &sync.RWMutex{},
		clusterLogIndexMap: make(map[string]int),
		healthChecker:      newHealthChecker(),
		heartbeatC:         make(chan bool),
		toCandidateC:       make(chan bool),
		toLeaderC:          make(chan bool),
	}

	clusters := strings.Split(cluster, ",")
	for _, v := range clusters {
		// Ignore myself
		if v != address {
			owl.nodes[v] = &node{address: v}
			owl.healthChecker.nodes[v] = -1
		}
	}

	owl.rpc(address)

	//go owl.clientHealthCheck()

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

	client, err := rpc.DialHTTP("tcp", o.nodes[address].address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", o.nodes[address].address, err)
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
	client, err := rpc.DialHTTP("tcp", o.nodes[address].address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", o.nodes[address].address, err)
		return nil, errRPCConnectFailed
	}
	o.clients[address] = client

	return client, err
}

func (o *Owl) getVoteFor() string {
	o.voteLock.RLock()
	defer o.voteLock.RUnlock()
	return o.votedFor
}

func (o *Owl) setVoteFor(target string) {
	o.voteLock.Lock()
	o.votedFor = target
	o.voteLock.Unlock()
}

func (o *Owl) isHealthy() bool {
	return o.healthChecker.isHealthy()
}

func (o *Owl) start() {
	o.state = Follower
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
		case Follower:
			glog.V(10).Infof("follower-%s is a follower", o.me)
			select {
			case <-o.heartbeatC:
				glog.V(10).Infof("follower-%s receives heartbeat", o.me)

			case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%s receives heartbeat timeout, Follower => PreCandidate", o.me)
				o.state = PreCandidate
			}

		case PreCandidate:
			glog.V(4).Infof("follower-%s is a pre candidate, pre vote for myself, my term: %d", o.me, o.currentTerm)
			o.leaderAddress = ""
			o.setVoteFor("")
			o.preVoteCount = 1
			go o.broadcastRequestVote(true)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%s requests pre vote timeout, PreCandidate => Follower", o.me)
				o.state = Follower
			case <-o.toCandidateC:
				glog.V(4).Infof("follower-%s wins the pre vote, PreCandidate => Candidate", o.me)
				o.state = Candidate
			}

		case Candidate:
			o.currentTerm++
			o.setVoteFor(o.address)
			o.voteCount = 1
			glog.V(4).Infof("follower-%s is a candidate, vote for myself, my term: %d", o.me, o.currentTerm)

			go o.broadcastRequestVote(false)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%s requests vote timeout, Candidate => Follower", o.me)
				o.state = PreCandidate
			case <-o.toLeaderC:
				glog.V(4).Infof("follower-%s wins the vote, Candidate => Leader", o.me)

				o.clusterLock.Lock()
				for _, i := range o.nodes {
					o.clusterLogIndexMap[i.address] = -1
				}
				o.clusterLock.Unlock()

				o.state = Leader
			}

		case Leader:
			glog.V(10).Infof("follower-%s is a leader, send heartbeat", o.me)
			o.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (o *Owl) serveChannels() {
	for {
		select {
		case food := <-o.beak.proposeC:
			glog.V(4).Infof("ROLE: %d, FOOD: %+v", o.state, food)
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
		}
	}
}

func (o *Owl) processData(msg string) error {
	if !o.isHealthy() {
		return errClusterNotHealthy
	}
	o.maw.addLog(o.currentTerm, msg)
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
)

type VoteReply struct {
	CurrentTerm       int
	CommittedLogIndex int
	VoteGranted       bool
	Reason            VoteReason
}

func (o *Owl) broadcastRequestVote(preVote bool) {
	var (
		args = VoteArgs{
			CurrentTerm:       o.currentTerm,
			CommittedLogIndex: o.committedLogIndex,
			CandidateID:       o.me,
			PreVote:           preVote,
		}
	)

	if preVote {
		args.CurrentTerm++
	}

	for _, i := range o.nodes {
		go func(i string) {
			var (
				reply = new(VoteReply)
			)
			o.sendRequestVote(i, args, reply)
		}(i.address)
	}
}

func (o *Owl) sendRequestVote(serverAddress string, args VoteArgs, reply *VoteReply) {
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

			if o.preVoteCount >= len(o.nodes)/2+1 {
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

			if o.voteCount >= len(o.nodes)/2+1 {
				o.toLeaderC <- true
			}
		}
	}
}

func (o *Owl) RequestVote(args VoteArgs, reply *VoteReply) error {
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
	PrevLogIndex   int
	PrevLogTerm    int
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
		var (
			args = HeartbeatArgs{
				Idx:            idx,
				Term:           o.currentTerm,
				Me:             o.me,
				CurrentLogTerm: o.getLastTerm(),
			}
		)

		o.clusterLock.RLock()
		prevLogIndex := o.clusterLogIndexMap[i.address]
		o.clusterLock.RUnlock()
		//glog.V(4).Infof("NODE: %v, PREVLOGINDEX: %d, LASTLOGINDEX: %d",
		//	i, o.getLastLogIndex(), prevLogIndex)
		if o.getLastLogIndex() > prevLogIndex {
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = o.maw.getLastTerm()
			args.Logs = o.maw.getLogsSince(prevLogIndex)
		}

		go func(i string, args HeartbeatArgs) {
			var (
				reply = new(HeartbeatReply)
			)
			o.sendHeartbeat(i, args, reply)
		}(i.address, args)
	}
}

func (o *Owl) sendHeartbeat(serverAddress string, args HeartbeatArgs, reply *HeartbeatReply) {
	client, err := o.getClient(serverAddress)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %s with error %v", serverAddress, err)
		return
	}

	glog.V(10).Infof("sendHeartbeat to %s, ARGS: %+v", serverAddress, args)
	err = client.Call("Owl.Heartbeat", args, &reply)
	if err != nil {
		glog.V(4).Infof("failed to call %s with error %v", "Owl.Heartbeat", err)
		return
	}

	if reply.Success {
		if reply.CurrentLogIndex >= -1 {
			o.clusterLock.Lock()
			o.clusterLogIndexMap[serverAddress] = reply.CurrentLogIndex
			o.clusterLock.Unlock()
		}
		o.healthChecker.Add(reply.Me, reply.Idx)
	} else {
		glog.V(4).Infof("sendHeartbeat REPLY from %s: ARGS: %+v, REPLY: %+v", serverAddress, args, reply)
		// TWO CASES:
		// 1. its term is higher than mine, we need to become the follower
		// 2. the prev log index is not correct, do nothing, next time heartbeat we will send using the correct index
		if reply.Term > o.currentTerm {
			o.currentTerm = reply.Term
			o.state = Follower
			o.votedFor = ""
		}
	}
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
		o.currentTerm = args.Term
		o.state = Follower
		o.setVoteFor("")
	}

	o.leaderAddress = args.Me
	o.heartbeatC <- true

	// Empty heartbeat, just tell leader our current log term
	if len(args.Logs) == 0 {
		reply.Success = true
		reply.Term = o.currentTerm
		reply.CurrentLogIndex = o.getLastLogIndex()
		return nil
	}

	//sendHeartbeat REPLY from 127.0.0.1:10333:
	//ARGS: {Idx:1238 Term:1 Me:127.0.0.1:10111 CurrentLogTerm:0 Logs:[0xc000044cc0 0xc0002a4580] PrevLogIndex:0 PrevLogTerm:1},
	//REPLY: &{Me:127.0.0.1:10333 Idx:1238 Term:1 Success:false CurrentLogIndex:-1}

	// pre log index is correct, let's process the new maw and return the latest log term to leader
	// otherwise the pre log index is not match, do nothing but tell leader our current log term
	if args.PrevLogIndex == o.getLastLogIndex() {
		o.maw.addLogs(args.Logs)
		o.committedLogIndex = o.getLastLogIndex()
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
