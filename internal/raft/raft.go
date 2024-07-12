package raft

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
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
	timerCh 	chan struct{}		
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

	go rf.electionTimeout()
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

func (rf *Raft) electionTimeout() {
	start := time.Now()
	timeout := time.Duration(300 + rand.Intn(200)) * time.Millisecond
	for {
		select {
		case <-rf.timerCh:
			return
		default:
			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed > timeout {
				if rf.state == Candidate {
					rf.state = Follower
				}
				go rf.startElection()
				go rf.electionTimeout()
				return
			}
		}		
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	args := &AppendEntriesRequest{
		Term: rf.currentTerm,
	}
	rf.mu.Unlock()
	start := time.Now()
	timeout := time.Duration(30) * time.Millisecond

	for {
		now := time.Now()
		elapsed := now.Sub(start)
		if elapsed >= timeout {

			var wg sync.WaitGroup
			for _, peer := range rf.peers {
				wg.Add(1)
				if peer != rf.peers[rf.me] {
					go func(peer string) {
						defer wg.Done()
						var reply AppendEntriesResponse
						if rf.sendAppendEntry(peer, args, &reply) {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if reply.Term > rf.currentTerm && !reply.Success {
								rf.state = Follower
								rf.votedFor = -1
							}
						}
					}(peer)
				}
			}

			wg.Wait()
			if rf.state == Follower {
				go rf.electionTimeout()
				return
			}
			start = now
		}
	}
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
	var cond	sync.Cond

	for _, peer := range rf.peers {
		if peer != rf.peers[rf.me] {
			go func(peer string) {
				var reply RequestVoteResponse
				if rf.sendRequestVote(peer, args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
					} else if reply.Term == rf.currentTerm && reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
					cond.Broadcast()
				}
			}(rf.peers[rf.me])
		}
	}

	rf.mu.Lock()
	for votes <= int32(len(rf.peers) / 2) {
		cond.Wait()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
	}

	rf.timerCh <- struct{}{}
	rf.state = Leader
	go rf.sendHeartbeats()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntry(peer string, req *AppendEntriesRequest, res *AppendEntriesResponse) bool {
	return call(peer, "Raft.HandleAppendEntry", req, res)
}

func (rf *Raft) sendRequestVote(peer string, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	return call(peer, "Raft.HandleRequestVote", req, res)
}

func call(peer, rpcname string, req interface{}, res interface{}) bool {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Fatalf("Failed to dial %s: %s", peer, err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	if err := client.Call(rpcname, req, &res); err != nil {
		log.Fatalf("RPC call failed: %s", err)
		return false
	}

	return true
}

func (rf *Raft) HandleAppendEntry(
	appendEntryReq *AppendEntriesRequest,
	appendEntryRes *AppendEntriesResponse,
) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > appendEntryReq.Term {
		appendEntryRes.Term = rf.currentTerm
		appendEntryRes.Success = false
		return nil
	}

	rf.currentTerm = appendEntryReq.Term
	appendEntryRes.Term = rf.currentTerm
	appendEntryRes.Success = true
	rf.timerCh <- struct{}{}
	go rf.electionTimeout()
	return nil
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