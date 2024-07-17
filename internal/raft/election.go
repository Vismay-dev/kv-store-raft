package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

func (rf *Raft) electionTimeout() {
	start := time.Now()
	timeout := time.Duration(350+rand.Intn(150)) * time.Millisecond

	for {
		if rf.killed() {
			rf.withLock(func() {
				utils.Dprintf(
					"[%d @ %s] node is dead...\n",
					rf.me,
					rf.peers[rf.me],
				)
			})
			continue
		}
		select {
		case <-rf.timerChElection:
			var isNotLeader bool = false

			rf.withLock(func() {
				if rf.state != Leader {
					isNotLeader = true
				}
			})

			if isNotLeader {
				go rf.electionTimeout()
			}
			return
		default:
			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed > timeout {
				rf.withLock(func() {
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
				})
				go rf.startElection()
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	var totalPeers int
	var peerAddrs []string
	var args *RequestVoteRequest
	var me int

	rf.withLock(func() {
		rf.state = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
		me = rf.me
		args = &RequestVoteRequest{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		peerAddrs = make([]string, len(rf.peers))
		copy(peerAddrs, rf.peers)
		totalPeers = len(peerAddrs)
	})

	go rf.electionTimeout()

	var cond = sync.NewCond(&rf.mu)

	var votes int32 = 1
	for _, peer := range peerAddrs {
		if peer != peerAddrs[me] {
			go func(peer string) {
				var reply RequestVoteResponse
				if rf.sendRequestVote(peer, args, &reply) {
					rf.withLock(func() {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
						} else if reply.Term == rf.currentTerm && reply.VoteGranted {
							atomic.AddInt32(&votes, 1)
						}
						cond.Broadcast()
					})
				}
			}(peer)
		}
	}

	var isNotCandidate bool = false

	rf.withLock(func() {
		for atomic.LoadInt32(&votes) <= int32(totalPeers/2) {
			if rf.state != Candidate {
				utils.Dprintf(
					"[%d @ %s] unqualified to become leader; quitting election\n",
					rf.me,
					rf.peers[rf.me],
				)
				isNotCandidate = true
				break
			}
			cond.Wait()
		}
	})

	if isNotCandidate {
		return
	}

	var argsNew *AppendEntriesRequest

	rf.withLock(func() {
		rf.timerChElection <- struct{}{}
		rf.state = Leader

		var prevLogTerm int
		if len(rf.log) > 0 {
			prevLogTerm = rf.log[len(rf.log)-1]["term"].(int)
		} else {
			prevLogTerm = 0
		}

		argsNew = &AppendEntriesRequest{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log),
			PrevLogTerm:  prevLogTerm,
			Entries:      make([]map[string]interface{}, 0),
			LeaderCommit: rf.commitIndex,
		}

		nextIndexInit := len(rf.log) + 1
		rf.nextIndex = []int{nextIndexInit, nextIndexInit, nextIndexInit, nextIndexInit, nextIndexInit}
		rf.matchIndex = []int{0, 0, 0, 0, 0}
	})

	rf.sendAppendEntries(argsNew)
	go rf.sendHeartbeats()
}

// RPC

func (rf *Raft) sendRequestVote(
	peer string, req *RequestVoteRequest,
	res *RequestVoteResponse,
) bool {
	var peerId int
	rf.withLock(func() {
		for i, rfPeer := range rf.peers {
			if rfPeer == peer {
				peerId = i
			}
		}
	})
	rpcname := fmt.Sprintf("Raft-%d.HandleRequestVote", peerId)
	return rf.call(peer, rpcname, &req, &res)
}

func (rf *Raft) HandleRequestVote(
	RequestVoteReq *RequestVoteRequest,
	RequestVoteRes *RequestVoteResponse,
) error {
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	var err error

	rf.withLock(func() {
		if rf.currentTerm > RequestVoteReq.Term {
			RequestVoteRes.Term = rf.currentTerm
			RequestVoteRes.VoteGranted = false
			err = nil
			return
		}

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

		err = nil
	})

	return err
}
