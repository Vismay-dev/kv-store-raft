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
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type Raft struct {
	mu          sync.Mutex
	state       State
	me          int
	currentTerm int
	votedFor    int
	peers       []string
	shutdownCh  chan struct{}
	timerCh     chan struct{}
}

func Make(peers []string, me int) *Raft {
	rf := &Raft{
		state:       Follower,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		peers:       peers,
		shutdownCh:  make(chan struct{}),
		timerCh:     make(chan struct{}),
	}

	rf.serve()

	return rf
}

func (rf *Raft) serve() {
	serviceName := fmt.Sprintf("Raft-%d", rf.me)
	if err := rpc.RegisterName(serviceName, rf); err != nil {
		log.Fatalf("Failed to register RPC: %s", err)
	}

	listener, err := net.Listen("tcp", rf.peers[rf.me])
	if err != nil {
		log.Fatalf("Failed to listen on %s: %s", rf.peers[rf.me], err)
	}

	go func(listener net.Listener) {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf("Failed to accept connection: %s", err)
			}
			go rpc.ServeConn(conn)
		}
	}(listener)
}

func (rf *Raft) electionTimeout() {
	start := time.Now()
	timeout := time.Duration(200+rand.Intn(300)) * time.Millisecond

	for {
		select {
		case <-rf.timerCh:
			return
		default:
			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed > timeout {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state == Candidate {
					rf.state = Follower
					log.Printf(
						"[%d @ %s] restarting election...\n",
						rf.me,
						rf.peers[rf.me],
					)
				} else {
					log.Printf(
						"[%d @ %s] starting election...\n",
						rf.me,
						rf.peers[rf.me],
					)
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
		Term:     rf.currentTerm,
		LeaderId: rf.me,
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
				if peer != rf.peers[rf.me] {
					wg.Add(1)
					go func(peer string) {
						defer wg.Done()
						var reply AppendEntriesResponse
						if rf.sendAppendEntry(peer, args, &reply) {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if reply.Term > rf.currentTerm && !reply.Success {
								rf.state = Follower
								rf.votedFor = -1
								log.Printf(
									"[%d @ %s] (leader) heartbeat failed; reverting to follower\n",
									rf.me,
									rf.peers[rf.me],
								)
							}
						}
					}(peer)
				}
			}
			wg.Wait() // check: does the leader have to wait for all responses?

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
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	var votes int32
	cond := sync.NewCond(&rf.mu)

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
			}(peer)
		}
	}

	rf.mu.Lock()
	for votes <= int32(len(rf.peers)/2) {
		cond.Wait()
		if rf.state != Candidate {
			log.Printf(
				"[%d @ %s] unqualified to become leader; quitting election\n",
				rf.me,
				rf.peers[rf.me],
			)
			rf.mu.Unlock()
			return
		}
	}

	log.Printf(
		"[%d @ %s] secured election; converting to leader\n",
		rf.me,
		rf.peers[rf.me],
	)

	rf.timerCh <- struct{}{}
	rf.state = Leader
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
	return call(peer, rpcname, &req, &res)
}

func (rf *Raft) sendRequestVote(peer string, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	var peerId int
	for i, rfPeer := range rf.peers {
		if rfPeer == peer {
			peerId = i
		}
	}
	rpcname := fmt.Sprintf("Raft-%d.HandleRequestVote", peerId)
	return call(peer, rpcname, &req, &res)
}

func call(peer, rpcname string, req interface{}, res interface{}) bool {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Fatalf("Failed to dial %s: %s", peer, err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	if err := client.Call(rpcname, req, res); err != nil {
		log.Fatalf("RPC call failed: %s", err)
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

	if rf.currentTerm > AppendEntryReq.Term {
		log.Printf(
			"[%d @ %s] rejecting AppendEntry RPC from leader %d\n",
			rf.me,
			rf.peers[rf.me],
			AppendEntryReq.LeaderId,
		)

		AppendEntryRes.Term = rf.currentTerm
		AppendEntryRes.Success = false
		return nil
	}

	rf.state = Follower
	rf.currentTerm = AppendEntryReq.Term
	AppendEntryRes.Term = rf.currentTerm
	AppendEntryRes.Success = true
	rf.timerCh <- struct{}{}
	go rf.electionTimeout()
	return nil
}

func (rf *Raft) HandleRequestVote(
	RequestVoteReq *RequestVoteRequest,
	RequestVoteRes *RequestVoteResponse,
) error {
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

		log.Printf(
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
		rf.timerCh <- struct{}{}
		go rf.electionTimeout()
	}
	return nil
}
