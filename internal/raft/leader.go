package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

func (rf *Raft) sendHeartbeats() {
	start := time.Now()
	timeout := time.Duration(40) * time.Millisecond

	for {
		select {
		case <-rf.timerChHb:
			go rf.sendHeartbeats()
			return
		default:
			var isNotLeader bool = false
			var args *AppendEntriesRequest
			var killed bool = false

			rf.withLock(func() {
				if rf.state != Leader {
					go rf.electionTimeout()
					isNotLeader = true
					return
				}

				if rf.killed() {
					utils.Dprintf(
						"[%d @ %s] node is dead; try heartbeat again later\n",
						rf.me,
						rf.peers[rf.me],
					)
					killed = true
					return
				}

				var prevLogTerm int
				var prevLogIndex int = len(rf.log)
				if len(rf.log) > 0 {
					prevLogTerm = rf.log[len(rf.log)-1]["term"].(int)
				} else {
					prevLogTerm = 0
				}

				args = &AppendEntriesRequest{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      make([]map[string]interface{}, 0),
					LeaderCommit: rf.commitIndex,
				}
			})

			if isNotLeader {
				return
			}

			if killed {
				return
			}

			now := time.Now()
			elapsed := now.Sub(start)
			if elapsed >= timeout {
				rf.sendAppendEntries(args)
				start = now
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesRequest) {
	var peerAddrs []string
	var me int

	rf.withLock(func() {
		peerAddrs = make([]string, len(rf.peers))
		copy(peerAddrs, rf.peers)
		me = rf.me
	})

	var wg sync.WaitGroup

	for i, peer := range peerAddrs {
		if peer != peerAddrs[me] {
			wg.Add(1)
			go func(peerAddr string, idx int, rpcArgs AppendEntriesRequest) {
				defer wg.Done()
				var reply AppendEntriesResponse

				rf.withLock(func() {
					if rf.nextIndex[idx] <= len(rf.log) {
						utils.Dprintf(
							"[%d @ %s] log coherence issue detected for follower @ %s\n",
							rf.me,
							rf.peers[rf.me],
							peerAddr,
						)
						rpcArgs.Entries = rf.log[rf.nextIndex[idx]-1:]
						rpcArgs.PrevLogIndex = rf.nextIndex[idx] - 1
						rpcArgs.PrevLogTerm = rf.log[rpcArgs.PrevLogIndex]["term"].(int)
					}
				})

				if rf.sendAppendEntry(peer, &rpcArgs, &reply) {
					rf.withLock(func() {
						if reply.Term > rf.currentTerm && !reply.Success {
							rf.state = Follower
							rf.votedFor = -1
							utils.Dprintf(
								"[%d @ %s] (leader) heartbeat failed; reverting to follower\n",
								rf.me,
								rf.peers[rf.me],
							)
						} else if reply.Term <= rf.currentTerm && !reply.Success {
							rf.nextIndex[idx] -= 1
						} else if reply.Success {
							rf.matchIndex[idx] = len(rf.log)
							rf.nextIndex[idx] = len(rf.log) + 1
						}
					})
				} else {
					rf.withLock(func() {
						utils.Dprintf(
							"[%d @ %s] trying to send a heartbeat here to %s\n",
							rf.me,
							rf.peers[rf.me],
							peerAddr,
						)
					})
				}
			}(peer, i, *args)
		}
	}
}

// RPC

func (rf *Raft) sendAppendEntry(
	peer string, req *AppendEntriesRequest,
	res *AppendEntriesResponse,
) bool {
	var peerId int

	rf.withLock(func() {
		for i, rfPeer := range rf.peers {
			if rfPeer == peer {
				peerId = i
			}
		}
	})

	rpcname := fmt.Sprintf("Raft-%d.HandleAppendEntry", peerId)
	return rf.call(peer, rpcname, &req, &res)
}

func (rf *Raft) SendData(
	ClientReqReq *ClientReqRequest,
	ClientReqRes *ClientReqResponse,
) error {
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	var rfState State
	rf.withLock(func() {
		rfState = rf.state
	})

	if rfState != Leader {
		var peer string
		var rpcname string

		rf.withLock(func() {
			for i, rfPeer := range rf.peers {
				if i == rf.leaderId {
					peer = rfPeer
				}
			}
			rpcname = fmt.Sprintf("Raft-%d.SendData", rf.leaderId)
		})

		if !rf.call(peer, rpcname, ClientReqReq, ClientReqRes) {
			err := fmt.Errorf("error forward request to leader node")
			return err
		}

		return nil
	}

	var me int
	var currentTerm int
	var peerAddrs []string
	var prevLogTerm int
	var prevLogIndex int
	var commitIndex int

	rf.withLock(func() {
		rf.timerChHb <- struct{}{}

		peerAddrs = make([]string, len(rf.peers))
		copy(peerAddrs, rf.peers)
		me = rf.me
		currentTerm = rf.currentTerm

		prevLogIndex = len(rf.log)
		if len(rf.log) > 0 {
			prevLogTerm = rf.log[len(rf.log)-1]["term"].(int)
		} else {
			prevLogTerm = 0
		}

		rf.log = append(rf.log, ClientReqReq.Entries...)
		commitIndex = rf.commitIndex
	})

	var replicationCount int32 = 1

	for i, peer := range peerAddrs {
		if peer != peerAddrs[me] {
			go func(peer string, prevLogIndex int, prevLogTerm int, idx int, commitIndex int) {
				var args *AppendEntriesRequest

				rf.withLock(func() {
					args = &AppendEntriesRequest{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      rf.log[rf.nextIndex[idx]-1:],
						LeaderCommit: commitIndex,
					}
				})

				var reply AppendEntriesResponse
				ok := rf.sendAppendEntry(peer, args, &reply)

				for !ok && reply.Term == currentTerm {
					rf.withLock(func() {
						args.PrevLogIndex = prevLogIndex - 1
						args.PrevLogTerm = rf.log[prevLogIndex-2]["term"].(int)
						args.Entries = rf.log[prevLogIndex-1:]
					})

					ok = rf.sendAppendEntry(peer, args, &reply)
				}

				atomic.AddInt32(&replicationCount, 1)

				rf.withLock(func() {
					rf.matchIndex[idx] = len(rf.log)
					rf.nextIndex[idx] = len(rf.log) + 1
				})
			}(peer, prevLogIndex, prevLogTerm, i, commitIndex)
		}
	}

	for atomic.LoadInt32(&replicationCount) < int32(len(peerAddrs)/2) {
	}

	rf.withLock(func() {
		rf.timerChHb <- struct{}{}
		rf.matchIndex[rf.me] = len(rf.log)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.commitIndex = len(rf.log)
		ClientReqRes.CommitIndex = rf.commitIndex
		utils.Dprintf(
			"[%d @ %s] commit succesful\n",
			rf.me,
			rf.peers[rf.me],
		)
	})

	ClientReqRes.Success = true

	return nil
}
