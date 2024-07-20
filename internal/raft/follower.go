package raft

import (
	"fmt"
	"log"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

func (rf *Raft) HandleAppendEntry(
	AppendEntryReq *AppendEntriesRequest,
	AppendEntryRes *AppendEntriesResponse,
) error {
	if rf.killed() {
		return fmt.Errorf("node is dead")
	}

	var err error
	var shouldStartElec bool = false

	rf.withLock("", func() {
		utils.Dprintf(
			"[%d @ %s] received append entry from leader: %d\n",
			rf.me,
			rf.peers[rf.me],
			rf.leaderId,
		)

		if AppendEntryReq.Term > rf.currentTerm {
			utils.Dprintf(
				"[%d @ %s] found leader of a newer term; reverting to follower - - - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			rf.currentTerm = AppendEntryReq.Term
			rf.state = Follower
			rf.votedFor = -1
			shouldStartElec = true
		}

		if rf.currentTerm > AppendEntryReq.Term {
			utils.Dprintf(
				"[%d @ %s] rejecting AppendEntry RPC from leader - - - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			AppendEntryRes.Term = rf.currentTerm
			AppendEntryRes.Success = false
			err = nil
			return
		}

		if (AppendEntryReq.PrevLogIndex > 0 && len(rf.log) < AppendEntryReq.PrevLogIndex) ||
			(AppendEntryReq.PrevLogIndex > 0 && rf.log[AppendEntryReq.PrevLogIndex-1]["term"].(int) != AppendEntryReq.PrevLogTerm) {
			log.Printf(
				"[%d @ %s] rejecting AppendEntry RPC from leader - x - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			utils.Dprintf(
				"[%d @ %s] rejecting AppendEntry RPC from leader - x - %d\n",
				rf.me,
				rf.peers[rf.me],
				AppendEntryReq.LeaderId,
			)
			AppendEntryRes.Term = rf.currentTerm
			AppendEntryRes.Success = false
			AppendEntryRes.Reason = "LogInconsistency"
			err = nil
			return
		}

		if len(AppendEntryReq.Entries) > 0 {
			if AppendEntryReq.PrevLogIndex == 0 && len(rf.log) > 0 {
				utils.Dprintf(
					"[%d @ %s] clearing existing log entries\n",
					rf.me,
					rf.peers[rf.me],
				)
				rf.log = rf.log[:0]
			}
			rf.log = append(rf.log[:AppendEntryReq.PrevLogIndex], AppendEntryReq.Entries...)
			utils.Dprintf(
				"[%d @ %s] log added succesfully\n",
				rf.me,
				rf.peers[rf.me],
			)
		}

		if shouldStartElec {
			go rf.electionTimeout()
		}

		rf.timerChElection <- struct{}{}

		rf.state = Follower
		rf.currentTerm = AppendEntryReq.Term
		rf.leaderId = AppendEntryReq.LeaderId
		rf.commitIndex = min(AppendEntryReq.LeaderCommit, len(rf.log))

		AppendEntryRes.Term = rf.currentTerm
		AppendEntryRes.Success = true

		if err := rf.persist(); err != nil {
			log.Fatalf("Error persisting: %s\n", err)
		}

		err = nil
	})

	return err
}
