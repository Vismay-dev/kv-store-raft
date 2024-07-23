package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

func (rf *Raft) electionTimeout() {
	ticker := time.NewTicker(time.Duration(350+rand.Intn(250)) * time.Millisecond)

	for {
		select {
		case <-rf.timerChElection:
			var isNotLeader bool = false

			rf.withLock("", func() {
				if rf.state != Leader {
					isNotLeader = true
				}
			})

			if isNotLeader {
				go rf.electionTimeout()
			}
			return
		case <-ticker.C:
			if rf.killed() {
				// rf.withLock("", func(){
				// 	utils.Dprintf(
				// 		"[%d @ %s] this node has been killed\n",
				// 		rf.me,
				// 		rf.peers[rf.me],
				// 	)
				// })
				continue
			}
			rf.withLock("", func() {
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

func (rf *Raft) startElection() {
	var totalPeers int
	var peerAddrs []string
	var args *RequestVoteRequest
	var me int

	rf.withLock("", func() {
		rf.state = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me

		if err := rf.persist(); err != nil {
			log.Fatalf("Error persisting: %s\n", err)
		}

		me = rf.me

		var prevLogTerm int
		var prevLogIndex int = len(rf.log)
		if len(rf.log) > 0 {
			prevLogTerm = rf.log[len(rf.log)-1]["term"].(int)
		} else {
			prevLogTerm = 0
		}

		args = &RequestVoteRequest{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: prevLogIndex,
			LastLogTerm:  prevLogTerm,
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
					rf.withLock("", func() {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower

							if err := rf.persist(); err != nil {
								log.Fatalf("Error persisting: %s\n", err)
							}
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

	rf.withLock("", func() {
		for atomic.LoadInt32(&votes) <= int32(totalPeers/2) {
			if rf.state != Candidate || rf.currentTerm != args.Term {
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

	rf.withLock("", func() {
		rf.timerChElection <- struct{}{}
		rf.state = Leader
		rf.leaderId = rf.me

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

	go rf.sendAppendEntries(argsNew)
	go rf.sendHeartbeats()
}

// RPC

func (rf *Raft) sendRequestVote(
	peer string, req *RequestVoteRequest,
	res *RequestVoteResponse,
) bool {
	var peerId int
	rf.withLock("", func() {
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

	rf.withLock("", func() {
		if rf.currentTerm > RequestVoteReq.Term {
			RequestVoteRes.Term = rf.currentTerm
			RequestVoteRes.VoteGranted = false
			err = nil
			return
		}

		if rf.currentTerm < RequestVoteReq.Term {
			utils.Dprintf(
				"[%d @ %s] HERE %d\n",
				rf.me,
				rf.peers[rf.me],
				RequestVoteReq.CandidateId,
			)
			rf.currentTerm = RequestVoteReq.Term
			rf.votedFor = -1
			rf.state = Follower
		}

		lastLogIndex := len(rf.log)
		lastLogTerm := 0

		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1]["term"].(int)
		}

		if (rf.votedFor == -1 || rf.votedFor == RequestVoteReq.CandidateId) &&
			(RequestVoteReq.LastLogIndex > lastLogIndex || (RequestVoteReq.LastLogIndex == lastLogIndex && RequestVoteReq.LastLogTerm == lastLogTerm)) {
			rf.votedFor = RequestVoteReq.CandidateId
			RequestVoteRes.VoteGranted = true
			rf.timerChElection <- struct{}{}
			utils.Dprintf(
				"[%d @ %s] voting for candidate %d\n",
				rf.me,
				rf.peers[rf.me],
				RequestVoteReq.CandidateId,
			)
		} else {
			RequestVoteRes.VoteGranted = false
		}

		if err := rf.persist(); err != nil {
			log.Fatalf("Error persisting: %s\n", err)
		}

		RequestVoteRes.Term = rf.currentTerm

		err = nil
	})

	return err
}
