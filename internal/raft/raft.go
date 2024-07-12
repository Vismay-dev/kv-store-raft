package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
)

type State string

const (
	Follower 	State = "follower"
	Candidate 	State = "candidate"
	Leader 		State = "leader"
)
type Raft struct {
	mu 			sync.Mutex
	state 		State
	me 			int
	currentTerm int
	votedFor    int
	peers		[]string
	shutdownCh  chan struct{}
}

func Make(peers []string, me int) {
	rf := &Raft{
		state: Follower,
		me: me,
		currentTerm: 0,
		votedFor: -1,
		peers: peers,
		shutdownCh: make(chan struct{}),
	}

	rf.serve()
}

func (rf *Raft) serve() {
	rpc.Register(rf)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", rf.peers[rf.me])
	if err != nil {
		log.Fatalf("Failed to listen on %s: %s", rf.peers[rf.me], err)
	}
	defer listener.Close()

	go http.Serve(listener, nil)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteRequest{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	var votes	int32
	var wg		sync.WaitGroup

	for _, peer := range rf.peers {
		if peer != rf.peers[rf.me] {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				var reply RequestVoteResponse
				if rf.sendRequestVote(peer, args, &reply) {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
					} else if reply.Term == rf.currentTerm && reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
				}
			}(rf.peers[rf.me])
		}
	}

	wg.Wait()
	if rf.state == Candidate && votes > int32(len(rf.peers) / 2) {
		rf.state = Leader
		// start heartbeats goroutine
	}
	
}

func (rf *Raft) sendRequestVote(peer string, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Fatalf("Failed to dial %s: %s", peer, err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	if err := client.Call("Raft.HandleRequestVote", req, &res); err != nil {
		log.Fatalf("RPC call failed: %s", err)
		return false
	}

	return true
}

func (rf *Raft) HandleRequestVote(
	requestVoteReq *RequestVoteRequest, 
	requestVoteRes *RequestVoteResponse,
) error {	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > requestVoteReq.Term {
		requestVoteRes.Term = rf.currentTerm
		requestVoteRes.VoteGranted = false
		return nil
	}

	if rf.currentTerm < requestVoteReq.Term {
		rf.currentTerm = requestVoteReq.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == requestVoteReq.CandidateId {
		rf.votedFor = requestVoteReq.CandidateId
		requestVoteRes.VoteGranted = true
	} else {
		requestVoteRes.VoteGranted = false
	}

	requestVoteRes.Term = rf.currentTerm
	return nil
}