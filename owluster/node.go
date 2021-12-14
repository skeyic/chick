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
	return &Beak{proposeC: make(chan *food)}
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
	return m.logs[start:]
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

func (m *Maw) maybeSnapshot() {
	// Remove committed maw from the maw if there are already too many there
}

// Owl node
type Owl struct {
	// my id
	me int

	// process channel
	proposeC chan *food
	beak     *Beak

	// leader id
	leaderID int

	// other nodes except me
	nodes map[int]*node

	// rpc client to other nodes
	clientLock *sync.RWMutex
	clients    map[int]*rpc.Client

	// current state
	state State

	// current cluster health, has quorum or not
	isHealthy bool

	// current term
	currentTerm int

	// vote for who in this term, -1 for nobody
	votedFor int

	// get voted count in this term
	voteCount    int
	preVoteCount int

	// heartbeat channel
	heartbeatC chan bool

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
	currentLogIndexMap map[int]int
}

func NewRaft(id int, port, address string, beak *Beak) *Owl {
	raft := &Owl{
		me:         id,
		beak:       beak,
		proposeC:   beak.proposeC,
		maw:        newMaw(),
		nodes:      make(map[int]*node),
		clients:    make(map[int]*rpc.Client),
		clientLock: &sync.RWMutex{},

		currentLogIndexMap: make(map[int]int),
		heartbeatC:         make(chan bool),
		toCandidateC:       make(chan bool),
		toLeaderC:          make(chan bool),
	}

	clusters := strings.Split(address, ",")
	for k, v := range clusters {
		raft.nodes[k] = &node{address: v}
	}

	raft.rpc(port)

	//go raft.clientHealthCheck()

	raft.start()

	return raft
}

func (rf *Owl) rpc(port string) {
	err := rpc.Register(rf)
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

func (rf *Owl) getLastLogIndex() int {
	return rf.maw.getLastIndex()
}

func (rf *Owl) getLastTerm() int {
	return rf.currentTerm
}

func (rf *Owl) getClient(id int) (*rpc.Client, error) {
	rf.clientLock.RLock()
	client, ok := rf.clients[id]
	rf.clientLock.RUnlock()
	if ok && rf.sendHello(client) == nil {
		return client, nil
	}

	client, err := rpc.DialHTTP("tcp", rf.nodes[id].address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", rf.nodes[id].address, err)
		return nil, errRPCConnectFailed
	}
	rf.clientLock.Lock()
	rf.clients[id] = client
	rf.clientLock.Unlock()

	return client, err
}

func (rf *Owl) newClient(id int) (*rpc.Client, error) {
	rf.clientLock.Lock()
	defer rf.clientLock.Unlock()
	client, err := rpc.DialHTTP("tcp", rf.nodes[id].address)
	if err != nil {
		glog.V(10).Infof("failed to dial %s with error %v", rf.nodes[id].address, err)
		return nil, errRPCConnectFailed
	}
	rf.clients[id] = client

	return client, err
}

func (rf *Owl) start() {
	rf.state = Follower
	rf.currentTerm = 0
	rf.leaderID = -1
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	// state change and handle RPC
	go rf.step()
}

func (rf *Owl) step() {
	rand.Seed(time.Now().UnixNano())

	for {
		switch rf.state {
		case Follower:
			glog.V(10).Infof("follower-%d is a follower", rf.me)
			select {
			case <-rf.heartbeatC:
				glog.V(10).Infof("follower-%d receives heartbeat", rf.me)

			case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%d receives heartbeat timeout, Follower => PreCandidate", rf.me)
				rf.state = PreCandidate
			}

		case PreCandidate:
			glog.V(4).Infof("follower-%d is a pre candidate, pre vote for myself, my term: %d", rf.me, rf.currentTerm)
			rf.leaderID = -1
			rf.votedFor = -1
			rf.preVoteCount = 1
			go rf.broadcastRequestVote(true)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%d requests pre vote timeout, PreCandidate => Follower", rf.me)
				rf.state = Follower
			case <-rf.toCandidateC:
				glog.V(4).Infof("follower-%d wins the pre vote, PreCandidate => Candidate", rf.me)
				rf.state = Candidate
			}

		case Candidate:
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			glog.V(4).Infof("follower-%d is a candidate, vote for myself, my term: %d", rf.me, rf.currentTerm)

			go rf.broadcastRequestVote(false)

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%d requests vote timeout, Candidate => Follower", rf.me)
				rf.state = PreCandidate
			case <-rf.toLeaderC:
				glog.V(4).Infof("follower-%d wins the vote, Candidate => Leader", rf.me)

				for i := range rf.nodes {
					rf.currentLogIndexMap[i] = 0
				}

				rf.state = Leader
			}

		case Leader:
			glog.V(10).Infof("follower-%d is a leader, send heartbeat", rf.me)
			rf.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (rf *Owl) serveChannels() {
	for {
		select {
		case food := <-rf.proposeC:
			switch rf.state {
			case Follower:
				food.errorC <- rf.forwardDataToLeader(food.msg)
			case PreCandidate, Candidate:
				food.errorC <- errLeaderNotElected
			case Leader:
				food.errorC <- rf.processData(food.msg)
			}
		}
	}
}

func (rf *Owl) processData(msg string) error {
	if !rf.isHealthy {
		return errClusterNotHealthy
	}
	rf.maw.addLog(rf.currentTerm, msg)
	return nil
}

func (rf *Owl) forwardDataToLeader(msg string) error {
	return rf.sendDataToLeader(msg)
}

type VoteArgs struct {
	CurrentTerm       int
	CommittedLogIndex int
	CandidateID       int
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

func (rf *Owl) broadcastRequestVote(preVote bool) {
	var (
		args = VoteArgs{
			CurrentTerm:       rf.currentTerm,
			CommittedLogIndex: rf.committedLogIndex,
			CandidateID:       rf.me,
			PreVote:           preVote,
		}
	)

	if preVote {
		args.CurrentTerm++
	}

	for i := range rf.nodes {
		go func(i int) {
			var (
				reply = new(VoteReply)
			)
			rf.sendRequestVote(i, args, reply)
		}(i)
	}
}

func (rf *Owl) sendRequestVote(serverID int, args VoteArgs, reply *VoteReply) {
	client, err := rf.getClient(serverID)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %d with error %v", serverID, err)
		return
	}

	glog.V(4).Infof("sendRequestVote %d, ARGS: %+v", serverID, args)
	err = client.Call("Owl.RequestVote", args, reply)
	if err != nil {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.RequestVote", err)
		return
	}
	glog.V(4).Infof("REPLY: %+v, MY TERM: %d", reply, rf.currentTerm)

	if args.PreVote {
		if reply.VoteGranted {
			rf.preVoteCount++

			if rf.preVoteCount >= len(rf.nodes)/2+1 {
				rf.toCandidateC <- true
			}
		}
	} else {
		if reply.CurrentTerm > rf.currentTerm {
			rf.currentTerm = reply.CurrentTerm
			rf.state = Follower
			rf.votedFor = -1
			return
		}

		if reply.VoteGranted {
			rf.voteCount++

			if rf.voteCount >= len(rf.nodes)/2+1 {
				rf.toLeaderC <- true
			}
		}
	}
}

func (rf *Owl) RequestVote(args VoteArgs, reply *VoteReply) error {
	reply.CurrentTerm = rf.currentTerm
	reply.CommittedLogIndex = rf.getLastLogIndex()

	// My term is higher, reject
	if args.CurrentTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.Reason = MyTermHigher
		return nil
	}

	// My last log index is higher, reject
	if args.CommittedLogIndex < rf.getLastLogIndex() {
		reply.VoteGranted = false
		reply.Reason = MyCommittedLogIndexHigher
		return nil
	}

	if rf.votedFor == -1 {
		if !args.PreVote {
			rf.currentTerm = args.CurrentTerm
			rf.votedFor = args.CandidateID
		}

		reply.VoteGranted = true
		return nil
	}

	reply.Reason = AlreadyVoted
	reply.VoteGranted = false
	return nil
}

type HeartbeatArgs struct {
	Term     int
	LeaderID int

	CurrentLogTerm int
	Logs           []*LogEntry
	PrevLogIndex   int
	PrevLogTerm    int
}

type HeartbeatReply struct {
	Term            int
	Success         bool
	CurrentLogIndex int
}

func (rf *Owl) broadcastHeartbeat() {
	for i := range rf.nodes {
		var (
			args = HeartbeatArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me,
			}
		)

		prevLogIndex := rf.currentLogIndexMap[i]
		if rf.getLastLogIndex() > prevLogIndex {
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.maw.getLastTerm()
			args.Logs = rf.maw.getLogsSince(prevLogIndex)
		}

		go func(i int, args HeartbeatArgs) {
			var (
				reply = new(HeartbeatReply)
			)
			rf.sendHeartbeat(i, args, reply)
		}(i, args)
	}
}

func (rf *Owl) sendHeartbeat(serverID int, args HeartbeatArgs, reply *HeartbeatReply) {
	client, err := rf.getClient(serverID)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %d with error %v", serverID, err)
		return
	}

	glog.V(10).Infof("sendHeartbeat to %d, ARGS: %+v", serverID, args)
	err = client.Call("Owl.Heartbeat", args, &reply)
	if err != nil {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.Heartbeat", err)
		return
	}
	glog.V(10).Infof("sendHeartbeat REPLY from %d: REPLY: %+v", serverID, reply)

	if reply.Success {
		if reply.CurrentLogIndex > 0 {
			rf.currentLogIndexMap[serverID] = reply.CurrentLogIndex
		}
	} else {
		// TWO CASES:
		// 1. its term is higher than mine, we need to become the follower
		// 2. the prev log index is not correct, do nothing, next time heartbeat we will send using the correct index
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
	}
}

func (rf *Owl) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	// My term is higher
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// Leader's term is higher
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.leaderID = args.LeaderID
	rf.heartbeatC <- true

	// Empty heartbeat, just tell leader our current log term
	if len(args.Logs) == 0 {
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.CurrentLogIndex = rf.getLastLogIndex()
		return nil
	}

	// If the pre log index is not match, do nothing but tell leader our current log term
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.CurrentLogIndex = rf.getLastLogIndex()
		return nil
	}

	// pre log index is correct, let's process the new maw and return the latest log term to leader
	rf.maw.addLogs(args.Logs)
	rf.committedLogIndex = rf.getLastLogIndex()
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.CurrentLogIndex = rf.getLastLogIndex()

	return nil
}

type DataArgs struct {
	Data string
}

type DataReply struct {
	Success bool
}

func (rf *Owl) clientHealthCheck() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			for id := range rf.nodes {
				var (
					client *rpc.Client
					err    error
				)
				client, err = rf.getClient(id)
				if err != nil || rf.sendHello(client) != nil {
					client, err = rf.newClient(id)
				}
			}
		}
	}
}

func (rf *Owl) sendHello(client *rpc.Client) error {
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

func (rf *Owl) Hello(args DataArgs, reply *DataReply) error {
	reply.Success = true
	return nil
}

func (rf *Owl) sendDataToLeader(msg string) error {
	var (
		args = &DataArgs{
			Data: msg,
		}
		reply    = new(DataReply)
		leaderID = rf.leaderID
	)

	if leaderID == -1 {
		return errLeaderNotElected
	}

	client, err := rf.getClient(leaderID)
	if err != nil {
		glog.V(10).Infof("failed to get rpc client to node %d with error %v", leaderID, err)
		return errRPCConnectFailed
	}

	err = client.Call("Owl.ReceiveData", args, reply)
	if err != nil || !reply.Success {
		glog.V(10).Infof("failed to call %s with error %v", "Owl.ReceiveData", err)
		return errRPCConnectFailed
	}
	return nil
}

func (rf *Owl) ReceiveData(args DataArgs, reply *DataReply) error {
	if rf.processData(args.Data) == nil {
		reply.Success = true
	}
	return nil
}
