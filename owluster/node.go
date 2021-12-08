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

func NewRaft(id int, port, address string) *Raft {
	raft := &Raft{
		me:         id,
		nodes:      make(map[int]*node),
		heartbeatC: make(chan bool),
		toLeaderC:  make(chan bool),
	}

	clusters := strings.Split(address, ",")
	for k, v := range clusters {
		raft.nodes[k] = &node{address: v}
	}

	raft.rpc(port)

	raft.start()

	return raft
}

// State int
type State int

// Follower,Candidate,Leader
const (
	Follower State = iota + 1
	Candidate
	Leader
)

// Raft node
type Raft struct {
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
	voteCount int

	// heartbeat channel
	heartbeatC chan bool

	// to leader channel
	toLeaderC chan bool

	log         []LogEntry
	commitIndex int
	nextIndex   []int
	matchIndex  []int
}

func (rf *Raft) rpc(port string) {
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

func (rf *Raft) getLastIndex() int {
	var (
		rlen = len(rf.log)
	)
	if rlen == 0 {
		return 0
	}
	return rf.log[rlen-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	var (
		rlen = len(rf.log)
	)
	if rlen == 0 {
		return 0
	}
	return rf.log[rlen-1].LogTerm
}

func (rf *Raft) start() {
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	// state change and handle RPC
	go rf.step()
}

func (rf *Raft) step() {
	rand.Seed(time.Now().UnixNano())

	for {
		switch rf.state {
		case Follower:
			glog.V(4).Infof("follower-%d is a follower", rf.me)
			select {
			case <-rf.heartbeatC:
				glog.V(4).Infof("follower-%d receives heartbeat", rf.me)

			case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%d receives heartbeat timeout, Follower => Candidate", rf.me)
				rf.state = Candidate
			}

		case Candidate:
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			glog.V(4).Infof("follower-%d is a candidate, vote for myself, my term: %d", rf.me, rf.currentTerm)

			go rf.broadcastRequestVote()

			select {
			case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
				glog.V(4).Infof("follower-%d requests vote timeout, Candidate => Follower", rf.me)
				rf.state = Follower
			case <-rf.toLeaderC:
				glog.V(4).Infof("follower-%d wins the vote, Candidate => Leader", rf.me)
				rf.state = Leader

				rf.nextIndex = make([]int, len(rf.nodes))
				rf.matchIndex = make([]int, len(rf.nodes))

				for i := range rf.nodes {
					rf.nextIndex[i] = 1
					rf.matchIndex[i] = 0
				}

				go func() {
					i := 0
					for {
						i++
						rf.log = append(rf.log,
							LogEntry{
								LogTerm:  rf.currentTerm,
								LogIndex: i,
							})
						time.Sleep(3 * time.Second)
					}
				}()
			}

		case Leader:
			glog.V(4).Infof("follower-%d is a leader, send heartbeat", rf.me)
			rf.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

type VoteArgs struct {
	Term        int
	CandidateID int
}

type VoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) broadcastRequestVote() {
	var (
		args = VoteArgs{
			Term:        rf.currentTerm,
			CandidateID: rf.me,
		}
	)

	for i := range rf.nodes {
		go func(i int) {
			var (
				reply = new(VoteReply)
			)
			rf.sendRequestVote(i, args, reply)
		}(i)
	}
}

func (rf *Raft) sendRequestVote(serverID int, args VoteArgs, reply *VoteReply) {
	var (
		address = rf.nodes[serverID].address
	)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		glog.Errorf("failed to dial %s with error %v", address, err)
		return
	}
	defer client.Close()

	glog.V(4).Infof("sendRequestVote %s, ARGS: %+v, REPLY: %+v", address, args, reply)
	err = client.Call("Raft.RequestVote", args, reply)
	if err != nil {
		glog.Errorf("failed to call %s with error %v", "Raft.RequestVote", err)
		return
	}
	glog.V(4).Infof("REPLY: %+v, MY TERM: %d", reply, rf.currentTerm)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
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

type LogEntry struct {
	// current term
	LogTerm int

	// current index
	LogIndex int

	LogCMD interface{}
}

type HeartbeatArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type HeartbeatReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.nodes {
		var (
			args = HeartbeatArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
		)

		prevLogIndex := rf.nextIndex[i] - 1
		if rf.getLastIndex() > prevLogIndex {
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.log[prevLogIndex].LogTerm
			args.Entries = rf.log[prevLogIndex:]
			glog.V(4).Infof("send entries: %v", args.Entries)
		}

		go func(i int, args HeartbeatArgs) {
			var (
				reply = new(HeartbeatReply)
			)
			rf.sendHeartbeat(i, args, reply)
		}(i, args)
	}
}

func (rf *Raft) sendHeartbeat(serverID int, args HeartbeatArgs, reply *HeartbeatReply) {
	var (
		address = rf.nodes[serverID].address
	)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		glog.Errorf("failed to dial %s with error %v", address, err)
		return
	}
	defer client.Close()

	glog.V(4).Infof("sendHeartbeat %s", address)
	err = client.Call("Raft.Heartbeat", args, &reply)
	if err != nil {
		glog.Errorf("failed to call %s with error %v", "Raft.Heartbeat", err)
		return
	}
	glog.V(4).Infof("REPLY: %+v", reply)

	if reply.Success {
		if reply.NextIndex > 0 {
			rf.nextIndex[serverID] = reply.NextIndex
			rf.matchIndex[serverID] = rf.nextIndex[serverID] - 1
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
	}
}

func (rf *Raft) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.heartbeatC <- true
	if len(args.Entries) == 0 {
		reply.Success = true
		reply.Term = rf.currentTerm
		return nil
	}

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return nil
	}

	rf.log = append(rf.log, args.Entries...)
	rf.commitIndex = rf.getLastIndex()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = rf.getLastIndex() + 1

	return nil
}

func (rf *Raft) RequestVote(args VoteArgs, reply *VoteReply) error {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return nil
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return nil
}
