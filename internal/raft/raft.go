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

// 1) Accept client request to random node and redirect to leader [x]
// 2) Leader: appends new entries to log [x]
// 3) Leader: issues AppendEntries RPCs to followers [x]
// 4) Leader: checks to see if entries have been safely replicated on majority [x]
// if the AppendEntries RPC has failed on any follower continue retrying indefinitely
// if condition is met, proceed, but continue retrying in background ?? check this properly
// differentiate between rpc failing and follower being "killed"; if killed give up
// 5) Leader: marks them as committed, applies entries, and sends response to client

// THE FINER DETAILS:

// AppendEntries RPC:
// - when a majority have replicated entry, append commitIndex and apply to state
// - include this commitIndex in future AppendEntries RPCs (including heartbeats) so the other servers find out
// these otherservers can apply it to their state and set it as their commitindex
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

// type LogEntry struct {
// 	data  	string
// 	term  	int
// }

type Raft struct {
	mu              sync.Mutex
	state           State
	me              int
	currentTerm     int
	votedFor        int
	peers           []string
	timerChElection chan struct{}
	timerChHb       chan struct{}

	log         []map[string]interface{}
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	leaderId    int

	dead int32
}

func Make(peers []string, me int) *Raft {
	rf := &Raft{
		state:           Follower,
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		peers:           peers,
		timerChElection: make(chan struct{}, 1),
		timerChHb:       make(chan struct{}, 1),

		log:         make([]map[string]interface{}, 0),
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
	go rf.electionTimeout()
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		go rf.sendHeartbeats()
		return
	}
	rf.mu.Unlock()
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
	timeout := time.Duration(350+rand.Intn(150)) * time.Millisecond

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
				rf.mu.Unlock()
				go rf.electionTimeout()
				return
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
	start := time.Now()
	timeout := time.Duration(40) * time.Millisecond

	for {
		select {
		case <-rf.timerChHb:
			go rf.sendHeartbeats()
			return
		default:
			rf.mu.Lock()
			args := &AppendEntriesRequest{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      make([]map[string]interface{}, 0),
				LeaderCommit: rf.commitIndex,
			}
			if rf.killed() {
				utils.Dprintf(
					"[%d @ %s] node is dead; try heartbeat again later\n",
					rf.me,
					rf.peers[rf.me],
				)
				return
			}
			rf.mu.Unlock()
			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed >= timeout {
				if !rf.sendAppendEntries(args) {
					go rf.electionTimeout()
					return
				}
				start = now
			}
		}

	}
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesRequest) bool {
	var peerAddrs []string
	rf.mu.Lock()
	peerAddrs = rf.peers
	rf.mu.Unlock()
	var wg sync.WaitGroup
	for _, peer := range peerAddrs {
		if peer != peerAddrs[rf.me] {
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
	defer rf.mu.Unlock()
	if rf.state == Follower {
		return false
	}
	return true
}

func (rf *Raft) startElection() {
	var peerAddrs []string

	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	args := &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	peerAddrs = rf.peers
	rf.mu.Unlock()

	go rf.electionTimeout()

	var votes int32 = 1

	for _, peer := range peerAddrs {
		if peer != peerAddrs[rf.me] {
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
					rf.mu.Unlock()
				}
			}(peer)
		}
	}

	for votes <= int32(len(rf.peers)/2) {
		rf.mu.Lock()
		if rf.state != Candidate {
			utils.Dprintf(
				"[%d @ %s] unqualified to become leader; quitting election\n",
				rf.me,
				rf.peers[rf.me],
			)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	rf.mu.Lock()
	rf.timerChElection <- struct{}{}
	rf.state = Leader
	argsNew := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      make([]map[string]interface{}, 0),
		LeaderCommit: rf.commitIndex,
	}
	rf.nextIndex = []int{0, 0, 0, 0, 0}
	rf.mu.Unlock()
	rf.sendAppendEntries(argsNew)
	go rf.sendHeartbeats()
}

func (rf *Raft) sendAppendEntry(peer string, req *AppendEntriesRequest, res *AppendEntriesResponse) bool {
	var peerId int
	rf.mu.Lock()
	for i, rfPeer := range rf.peers {
		if rfPeer == peer {
			peerId = i
		}
	}
	rf.mu.Unlock()
	rpcname := fmt.Sprintf("Raft-%d.HandleAppendEntry", peerId)
	return rf.call(peer, rpcname, &req, &res)
}

func (rf *Raft) sendRequestVote(peer string, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	var peerId int
	rf.mu.Lock()
	for i, rfPeer := range rf.peers {
		if rfPeer == peer {
			peerId = i
		}
	}
	rf.mu.Unlock()
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

	rf.mu.Lock()
	var id int = rf.me
	var address string = rf.peers[rf.me]
	rf.mu.Unlock()

	client := rpc.NewClient(conn)
	if err := client.Call(rpcname, req, res); err != nil {
		utils.Dprintf(
			"[%d @ %s] RPC call to [%s] failed: %s",
			id, address,
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
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > AppendEntryReq.Term {
		utils.Dprintf(
			"[%d @ %s] rejecting AppendEntry RPC from leader - - - %d\n",
			rf.me,
			rf.peers[rf.me],
			AppendEntryReq.LeaderId,
		)
		AppendEntryRes.Term = rf.currentTerm
		AppendEntryRes.Success = false
		return nil
	}

	if len(AppendEntryReq.Entries) > 0 {
		if AppendEntryReq.PrevLogIndex == 0 && len(rf.log) > 0 {
			utils.Dprintf(
				"[%d @ %s] rejecting AppendEntry RPC from leader - - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			AppendEntryRes.Term = rf.currentTerm
			AppendEntryRes.Success = false
			return nil
		}
		if (AppendEntryReq.PrevLogIndex > 0 && len(rf.log) < AppendEntryReq.PrevLogIndex) ||
			(AppendEntryReq.PrevLogIndex > 0 && rf.log[AppendEntryReq.PrevLogIndex]["term"] != AppendEntryReq.PrevLogTerm) {
			utils.Dprintf(
				"[%d @ %s] rejecting AppendEntry RPC from leader - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			AppendEntryRes.Term = rf.currentTerm
			AppendEntryRes.Success = false
			return nil
		}
		rf.log = append(rf.log, AppendEntryReq.Entries...)
		rf.timerChHb <- struct{}{}
		utils.Dprintf(
			"[%d @ %s] log added succesfully\n",
			rf.me,
			rf.peers[rf.me],
		)
	}

	rf.timerChElection <- struct{}{}
	rf.state = Follower
	rf.currentTerm = AppendEntryReq.Term
	rf.leaderId = AppendEntryReq.LeaderId
	rf.commitIndex = min(AppendEntryReq.LeaderCommit, len(rf.log))

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
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

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

func (rf *Raft) SendData(
	ClientReqReq *ClientReqRequest,
	ClientReqRes *ClientReqResponse,
) error {
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}
	rf.mu.Lock()
	if rf.state != Leader {
		var peer string
		for i, rfPeer := range rf.peers {
			if i == rf.leaderId {
				peer = rfPeer
			}
		}
		rpcname := fmt.Sprintf("Raft-%d.SendData", rf.leaderId)
		rf.mu.Unlock()
		if !rf.call(peer, rpcname, ClientReqReq, ClientReqRes) {
			return fmt.Errorf("error forward request to leader node")
		}
	} else {
		rf.log = append(rf.log, ClientReqReq.Entries...)
		var commitIndex int = rf.commitIndex
		rf.mu.Unlock()
		var replicationCount int32 = 1
		for i, peer := range rf.peers {
			if peer != rf.peers[rf.me] {
				go func(peer string) {
					rf.mu.Lock()
					args := &AppendEntriesRequest{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      rf.log[rf.nextIndex[i]:],
						LeaderCommit: commitIndex,
					}
					rf.mu.Unlock()
					var reply AppendEntriesResponse
					if rf.sendAppendEntry(peer, args, &reply) {
						atomic.AddInt32(&replicationCount, 1)
					}
				}(peer)
			}
		}

		for atomic.LoadInt32(&replicationCount) < int32(len(rf.peers)/2) {
		}

		rf.mu.Lock()
		rf.commitIndex = len(rf.log)
		ClientReqRes.CommitIndex = rf.commitIndex
		rf.mu.Unlock()

		ClientReqRes.Success = true
		utils.Dprintf(
			"[%d @ %s] commit succesful\n",
			rf.me,
			rf.peers[rf.me],
		)
	}
	return nil
}
