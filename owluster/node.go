package owluster

import (
	"github.com/golang/glog"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

// node cluster node
type node struct {
	connect bool
	address string
}

// State int
type State int

// Follower,Candidate,Leader
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

	LogCMD interface{}
}

// Owl node
type Owl struct {
	// my id
	me int

	// other nodes except me
	nodes map[int]*node

	// current state
	state State

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

	//lock *sync.RWMutex

	// the main data which all the logEntries will be committed into
	data OwlData

	// the logs
	logs []*LogEntry

	// record the current log index from each node
	currentLogIndexMap map[int]int
}

func NewRaft(id int, port, address string) *Owl {
	raft := &Owl{
		me:                 id,
		nodes:              make(map[int]*node),
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
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].LogIndex
}

func (rf *Owl) getLastTerm() int {
	//rf.lock.RLock()
	//defer rf.lock.RUnlock()
	return rf.currentTerm
}

func (rf *Owl) start() {
	rf.state = Follower
	rf.currentTerm = 0
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
				rf.state = Follower
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

func (rf *Owl) process(log *LogEntry, sync bool) error {
	for {
		switch rf.state {
		case Follower:
		case Candidate:
			// Do nothing, waiting for the leader been elected
		case Leader:
			rf.appendLog(log)
			if sync {
				rf.waitingForCommitted(log)
			}
		}
	}
}

func (rf *Owl) forwardToLeader(log *LogEntry, sync bool) {

}

func (rf *Owl) appendLog(log *LogEntry) {
	rf.maybeSnapshot()
	rf.logs = append(rf.logs, log)
	rf.committedLogIndex = rf.getLastLogIndex()
}

func (rf *Owl) maybeSnapshot() {
	// Remove committed logs from the logs if there are already too many there
	if len(rf.logs) > 100 {

	}
}

func (rf *Owl) waitingForCommitted(log *LogEntry) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
		}
	}
}

//
//func (rf *Owl) LeaderProcessLog(log *LogEntry) error {
//	//rf.toCommitLogs = append(rf.toCommitLogs, log)
//
//	for {
//
//	}
//	rf.data.do(log)
//	return nil
//}

type VoteArgs struct {
	CurrentTerm       int
	CommittedLogIndex int
	CandidateID       int
	PreVote           bool
}

type VoteReply struct {
	CurrentTerm       int
	CommittedLogIndex int
	VoteGranted       bool
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
	var (
		address = rf.nodes[serverID].address
	)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		glog.Errorf("failed to dial %s with error %v", address, err)
		return
	}
	defer client.Close()

	glog.V(4).Infof("sendRequestVote %s, ARGS: %+v", address, args)
	err = client.Call("Owl.RequestVote", args, reply)
	if err != nil {
		glog.Errorf("failed to call %s with error %v", "Owl.RequestVote", err)
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
		return nil
	}

	// My last log index is higher, reject
	if args.CommittedLogIndex < rf.getLastLogIndex() {
		reply.VoteGranted = false
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
			args.PrevLogTerm = rf.logs[prevLogIndex].LogTerm
			args.Logs = rf.logs[prevLogIndex:]
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
	var (
		address = rf.nodes[serverID].address
	)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		glog.Errorf("failed to dial %s with error %v", address, err)
		return
	}
	defer client.Close()

	glog.V(10).Infof("sendHeartbeat to %s, ARGS: %+v", address, args)
	err = client.Call("Owl.Heartbeat", args, &reply)
	if err != nil {
		glog.Errorf("failed to call %s with error %v", "Owl.Heartbeat", err)
		return
	}
	glog.V(10).Infof("sendHeartbeat REPLY from %s: REPLY: %+v", address, reply)

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

	// pre log index is correct, let's process the new logs and return the latest log term to leader
	rf.logs = append(rf.logs, args.Logs...)
	rf.committedLogIndex = rf.getLastLogIndex()
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.CurrentLogIndex = rf.getLastLogIndex()

	return nil
}
