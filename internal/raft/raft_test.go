package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

type TestCase func(t *testing.T, raftNodes []*Raft, peerAddresses []string)

// functional tests

func TestRaft(t *testing.T) {
	raftNodes, peerAddrs := setup(t)
	for testName, testFunc := range map[string]TestCase{
		"TestLeaderElectionNormal": testLeaderElectionNormal,
		"TestLogReplicationNormal": testLogReplicationNormal,
		// "TestLeaderElectionNetworkPartition": testLeaderElectionNetworkPartition,
	} {
		t.Run(testName, func(t *testing.T) {
			testFunc(t, raftNodes, peerAddrs)
		})
	}
}

// integration tests

func testLeaderElectionNormal(t *testing.T, raftNodes []*Raft, peerAddrs []string) {
	// // Check for exactly 1 leader
	checkLeaderElection(t, raftNodes)
	// Check for equality of terms across nodes
	checkTermEquality(t, raftNodes)
}

func testLogReplicationNormal(t *testing.T, raftNodes []*Raft, peerAddrs []string) {
	var term int

	raftNodes[0].mu.Lock()
	term = raftNodes[0].currentTerm
	raftNodes[0].mu.Unlock()

	entries := []map[string]interface{}{
		{
			"data": "this should be the first",
			"term": term,
		},
		{
			"data": "this should be the second",
			"term": term,
		},
		{
			"data": "this should be the third",
			"term": term,
		},
		{
			"data": "this should be the fourth",
			"term": term,
		},
	}

	res, err := ClientSendData(entries)
	if err != nil {
		t.Errorf("Error sending client request to raft: %s", err)
	}

	time.Sleep(2 * time.Second)

	// Check for exactly 1 leader
	checkLeaderElection(t, raftNodes)
	// Check for equality of terms across nodes
	checkTermEquality(t, raftNodes)
	// Check for complete equality of logs across nodes
	checkLogConsistency(t, raftNodes)
	// Check for committed
	checkCommitted(t, raftNodes, res.CommitIndex)
}

func testLeaderElectionNetworkPartition(t *testing.T, raftNodes []*Raft, peerAddrs []string) {
	randNode := raftNodes[rand.Intn(len(raftNodes))]

	// log.Printf("[==tester==] Killing random raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	log.Printf("[==tester==] Killing random raft node")
	randNode.Kill()

	time.Sleep(2 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
	time.Sleep(2 * time.Second)

	// log.Printf("[==tester==] Reviving random raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	log.Printf("[==tester==] Reviving random raft node")
	randNode.Revive()

	time.Sleep(2 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
	time.Sleep(2 * time.Second)

	var leaderNode *Raft
	for _, node := range raftNodes {
		node.mu.Lock()
		if node.state == Leader {
			leaderNode = node
		}
		node.mu.Unlock()
	}

	log.Printf("[==tester==] Killling leader raft node (%d @ %s)", leaderNode.me, peerAddrs[leaderNode.me])
	log.Printf("[==tester==] Killling leader raft node")
	leaderNode.Kill()

	// utils.Debug.Store(1)
	// go func() {
	// 	go utils.LogStackTraces(2 * time.Second)
	// 	time.Sleep(5 * time.Second)
	// 	panic("hereeeeeevis")
	// }()

	time.Sleep(2 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
	time.Sleep(2 * time.Second)

	// log.Printf("[==tester==] Reviving former leader raft node (%d @ %s)", leaderNode.me, peerAddrs[leaderNode.me])
	log.Printf("[==tester==] Reviving former leader raft node")
	leaderNode.Revive()

	time.Sleep(2 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
}

// helpers / unit tests

func checkLeaderElection(t *testing.T, raftNodes []*Raft) {
	t.Helper()
	leaderCnt := 0

	for _, rf := range raftNodes {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			continue
		}
		if rf.state == Leader {
			leaderCnt += 1
		}
		rf.mu.Unlock()
	}

	if leaderCnt != 1 {
		t.Errorf("Failed Leader Election; expected 1 leader, got %d", leaderCnt)
	}
}

func checkTermEquality(t *testing.T, raftNodes []*Raft) {
	t.Helper()

	raftNodes[0].mu.Lock()
	term := raftNodes[0].currentTerm
	raftNodes[0].mu.Unlock()

	for _, rf := range raftNodes[1:] {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			continue
		}
		if rf.currentTerm != term {
			t.Errorf("Failed Leader Election; node with inconsistent term found (%d)", rf.me)
		}
		rf.mu.Unlock()
	}
}

func checkLogConsistency(t *testing.T, raftNodes []*Raft) {
	t.Helper()

	raftNodes[0].mu.Lock()
	log := raftNodes[0].log
	raftNodes[0].mu.Unlock()

	for _, node := range raftNodes[1:] {
		node.mu.Lock()
		if !areEqual(log, node.log) {
			t.Errorf("logs differ across nodes; comparing nodes 0 and %d", node.me)
		}
		node.mu.Unlock()
	}
}

func checkCommitted(t *testing.T, raftNodes []*Raft, commitIndex int) {
	t.Helper()

	for _, node := range raftNodes[:] {
		node.mu.Lock()
		if node.commitIndex != commitIndex {
			t.Errorf("commit index differs across nodes; comparing leader (%d) and %d (%d)", commitIndex, node.me, node.commitIndex)
		}
		node.mu.Unlock()
	}
}

func areEqual(arr1, arr2 []map[string]interface{}) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := 0; i < len(arr1); i++ {
		if !entryEqual(arr1[i], arr2[i]) {
			return false
		}
	}

	return true
}

func entryEqual(entry1, entry2 map[string]interface{}) bool {
	// Assuming "data" and "term" are the keys to compare
	data1, _ := entry1["data"].(string)
	data2, _ := entry2["data"].(string)
	term1, _ := entry1["term"].(int)
	term2, _ := entry2["term"].(int)

	return data1 == data2 && term1 == term2
}

func setup(t *testing.T) ([]*Raft, []string) {
	t.Helper()

	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	raftNodes := []*Raft{}
	for i := range peerAddresses {
		rf := Make(peerAddresses, i)
		raftNodes = append(raftNodes, rf)
	}

	for _, rf := range raftNodes {
		go rf.Start()
	}

	// Allowing initial leader election
	time.Sleep(2 * time.Second)
	return raftNodes, peerAddresses
}
