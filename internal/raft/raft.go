package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// TODO:

// 1) Accept client request to random node and redirect to leader
// 2) Leader: appends new entries to log
// 3) Leader: issues AppendEntries RPCs to followers
// 4) Leader: checks to see if entries have been safely replicated on majority
// if the AppendEntries RPC has failed on any follower continue retrying indefinitely
// if condition is met, proceed, but continue retrying in background ?? check this properly
// differentiate between rpc failing and follower being "killed"; if killed give up
// 5) Leader: marks them as committed, applies entries, and sends response to client

// THE FINER DETAILS:

// AppendEntries RPC:
// - when a majority have replicated entry, append commitIndex and apply to state
// - include this commitIndex in future AppendEntries RPCs (including heartbeats) so the other servers find out
// these otherservers can apply it to their state (they should already atleast have it as commitIndex: check later pointers)
// - include the last index and term before new entries, if follower can't find the last i & t in their log, they should reject the request
// - if rejection is caused by i & t not existing:
// ???
// - but if i & t exists but rejection is because nextIndex conflict with an existing entry:
// decrement nextIndex, now after this leader should try again at the nextIndex and see if the nextIndex and log match,
// if they don't match decrement and repeat, if they match delete all entries that follow and add all entries of the leader from nextIndex
// see (OTHER IMPORTANT DETAILS) for more clarity
// - follower: if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

// RequestVote RPC:
// - follower: in the section where we check for if votedFor is null or candidateId, also check if: candidate's log is atleast as upto date as follower's

// OTHER IMPORTANT DETAILS: (these seem blurry - read Raft paper) (!! what is matchIndex?)
// - wherever commitIndex is being appended, if commitIndex > lastApplied, increment lastApplied and apply it.
// - for leader: if last log index >= nextIndex (probably due to nextIndexing decrementing because of conflict)
// send AppendEntries RPCs to follower with logentries starting at nextIndex
// if it's successful here: update nextIndex and matchIndex for follower
// if it fails decrement nextIndex and retry
// - for leader: if there exists and index N such that N > commitIndex and a majority of matchIndex[i] >= N
// and log[N].term == currentTerm, then set commitIndex == N (yep no clue here, read 5.3, 5.4 ig)

type LogEntry struct {
	// data  	string
	// term  	string
}

type Raft struct {
	mu              sync.Mutex
	state           State
	me              int
	currentTerm     int
	votedFor        int
	peers           []string
	timerChElection chan struct{}

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	dead int32
}

func Make(peers []string, me int) *Raft {
	rf := &Raft{
		state:           Follower,
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		peers:           peers,
		timerChElection: make(chan struct{}),

		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   []int{},
		matchIndex:  []int{},

		dead: 0,
	}

	go rf.serve()

	return rf
}

func (rf *Raft) Start() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	go rf.electionTimeout()
	if rf.state == Leader {
		go rf.sendHeartbeats()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) Revive() {
	if rf.killed() {
		atomic.StoreInt32(&rf.dead, 0)
		rf.Start()
	}
}

func (rf *Raft) killed() bool {
	return int(atomic.LoadInt32(&rf.dead)) == 1
}

func (rf *Raft) serve() {
	serviceName := fmt.Sprintf("Raft-%d", rf.me)
	if err := rpc.RegisterName(serviceName, rf); err != nil {
		utils.Dprintf("Failed to register RPC: %s", err)
	}

	listener, err := net.Listen("tcp", rf.peers[rf.me])
	if err != nil {
		log.Fatalf("Failed to listen on %s: %s", rf.peers[rf.me], err)
	}

	go func() {
		defer func() {
			if err := listener.Close(); err != nil {
				log.Fatalf("Failed to close listener @ %s", listener.Addr().String())
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf("[%d @ %s] Failed to accept connection: %s", rf.me, rf.peers[rf.me], err)
			}
			go rpc.ServeConn(conn)
		}
	}()
}

func (rf *Raft) electionTimeout() {
	start := time.Now()
	timeout := time.Duration(250+rand.Intn(150)) * time.Millisecond

	for {
		if rf.killed() {
			utils.Dprintf(
				"[%d @ %s] node is dead...\n",
				rf.me,
				rf.peers[rf.me],
			)
			return
		}
		select {
		case <-rf.timerChElection:
			rf.mu.Lock()
			if rf.state != Leader {
				go rf.electionTimeout()
			}
			rf.mu.Unlock()
			return
		default:
			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed > timeout {
				rf.mu.Lock()
				if rf.state == Candidate {
					rf.state = Follower
					utils.Dprintf(
						"[%d @ %s] restarting election...\n",
						rf.me,
						rf.peers[rf.me],
					)
				} else {
					utils.Dprintf(
						"[%d @ %s] starting election...\n",
						rf.me,
						rf.peers[rf.me],
					)
				}
				rf.mu.Unlock()
				go rf.startElection()
				return
			}
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	args := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      rf.log,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	start := time.Now()
	timeout := time.Duration(25) * time.Millisecond

	for {
		if rf.killed() {
			utils.Dprintf(
				"[%d @ %s] node is dead; try heartbeat again later\n",
				rf.me,
				rf.peers[rf.me],
			)
			return
		}
		now := time.Now()
		elapsed := now.Sub(start)
		if elapsed >= timeout {
			if !rf.sendEmptyAppendEntries(args) {
				return
			}
			start = now
		}
	}
}

func (rf *Raft) sendEmptyAppendEntries(args *AppendEntriesRequest) bool {
	var wg sync.WaitGroup
	for _, peer := range rf.peers {
		if peer != rf.peers[rf.me] {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				var reply AppendEntriesResponse
				if rf.sendAppendEntry(peer, args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm && !reply.Success {
						rf.state = Follower
						rf.votedFor = -1
						utils.Dprintf(
							"[%d @ %s] (leader) heartbeat failed; reverting to follower\n",
							rf.me,
							rf.peers[rf.me],
						)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
	wg.Wait()

	rf.mu.Lock()
	if rf.state == Follower {
		go rf.electionTimeout()
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return true
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	go rf.electionTimeout()
	args := &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rf.mu.Unlock()

	var votes int32 = 1
	cond := sync.NewCond(&rf.mu)

	for _, peer := range rf.peers {
		if peer != rf.peers[rf.me] {
			go func(peer string) {
				var reply RequestVoteResponse
				if rf.sendRequestVote(peer, args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
					} else if reply.Term == rf.currentTerm && reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
					cond.Broadcast()
					rf.mu.Unlock()
				}
			}(peer)
		}
	}

	rf.mu.Lock()
	for votes <= int32(len(rf.peers)/2) {
		if rf.state != Candidate {
			utils.Dprintf(
				"[%d @ %s] unqualified to become leader; quitting election\n",
				rf.me,
				rf.peers[rf.me],
			)
			rf.mu.Unlock()
			return
		}
		cond.Wait()
	}

	rf.timerChElection <- struct{}{}
	rf.state = Leader
	argsNew := &AppendEntriesRequest{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	go rf.sendEmptyAppendEntries(argsNew)
	go rf.sendHeartbeats()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntry(peer string, req *AppendEntriesRequest, res *AppendEntriesResponse) bool {
	var peerId int
	for i, rfPeer := range rf.peers {
		if rfPeer == peer {
			peerId = i
		}
	}
	rpcname := fmt.Sprintf("Raft-%d.HandleAppendEntry", peerId)
	return rf.call(peer, rpcname, &req, &res)
}

func (rf *Raft) sendRequestVote(peer string, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	var peerId int
	for i, rfPeer := range rf.peers {
		if rfPeer == peer {
			peerId = i
		}
	}
	rpcname := fmt.Sprintf("Raft-%d.HandleRequestVote", peerId)
	return rf.call(peer, rpcname, &req, &res)
}

func (rf *Raft) call(peer, rpcname string, req interface{}, res interface{}) bool {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		utils.Dprintf("Failed to dial %s: %s", peer, err)
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	if err := client.Call(rpcname, req, res); err != nil {
		utils.Dprintf(
			"[%d @ %s] RPC call to [%s] failed: %s",
			rf.me, rf.peers[rf.me],
			peer,
			err,
		)
		return false
	}

	return true
}

func (rf *Raft) HandleAppendEntry(
	AppendEntryReq *AppendEntriesRequest,
	AppendEntryRes *AppendEntriesResponse,
) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	if rf.currentTerm > AppendEntryReq.Term {
		utils.Dprintf(
			"[%d @ %s] rejecting AppendEntry RPC from leader %d\n",
			rf.me,
			rf.peers[rf.me],
			AppendEntryReq.LeaderId,
		)
		AppendEntryRes.Term = rf.currentTerm
		AppendEntryRes.Success = false
		return nil
	}

	rf.timerChElection <- struct{}{}
	rf.state = Follower
	rf.currentTerm = AppendEntryReq.Term

	utils.Dprintf(
		"[%d @ %s] received AppendEntry RPC from leader %d\n",
		rf.me,
		rf.peers[rf.me],
		AppendEntryReq.LeaderId,
	)

	AppendEntryRes.Term = rf.currentTerm
	AppendEntryRes.Success = true

	utils.Dprintf(
		"[%d @ %s] AppendEntry RPC from leader successful: %d\n",
		rf.me,
		rf.peers[rf.me],
		AppendEntryReq.LeaderId,
	)

	return nil
}

func (rf *Raft) HandleRequestVote(
	RequestVoteReq *RequestVoteRequest,
	RequestVoteRes *RequestVoteResponse,
) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	if rf.currentTerm > RequestVoteReq.Term {
		RequestVoteRes.Term = rf.currentTerm
		RequestVoteRes.VoteGranted = false
		return nil
	}

	// note: important to grasp this
	if rf.currentTerm < RequestVoteReq.Term {
		rf.currentTerm = RequestVoteReq.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == RequestVoteReq.CandidateId {
		rf.votedFor = RequestVoteReq.CandidateId
		RequestVoteRes.VoteGranted = true

		utils.Dprintf(
			"[%d @ %s] voting for candidate %d\n",
			rf.me,
			rf.peers[rf.me],
			RequestVoteReq.CandidateId,
		)
	} else {
		RequestVoteRes.VoteGranted = false
	}

	RequestVoteRes.Term = rf.currentTerm
	if RequestVoteRes.VoteGranted {
		rf.timerChElection <- struct{}{}
	}
	return nil
}
