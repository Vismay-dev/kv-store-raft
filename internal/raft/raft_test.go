package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

type TestCase func(t *testing.T, raftNodes []*Raft, peerAddresses []string)

// functional tests

func TestRaft(t *testing.T) {
	raftNodes, peerAddrs := setup(t)
	for testName, testFunc := range map[string]TestCase{
		// "TestLeaderElectionNormal": testLeaderElectionNormal,
		"TestLeaderElectionNetworkPartition": testLeaderElectionNetworkPartition,
		// "TestLogReplication": testLogReplication,
	} {
		t.Run(testName, func(t *testing.T) {
			testFunc(t, raftNodes, peerAddrs)
		})
	}
}

// integration tests

func testLeaderElectionNormal(t *testing.T, raftNodes []*Raft, _ []string) {
	// // Check for exactly 1 leader
	checkLeaderElection(t, raftNodes)
	// Check for equality of terms across nodes
	checkTermEquality(t, raftNodes)
}

func testLeaderElectionNetworkPartition(t *testing.T, raftNodes []*Raft, _ []string) {
	var randNode *Raft
	var randNodeState State

	var idx int = rand.Intn(len(raftNodes))
	randNode = raftNodes[idx]
	randNode.withLock("", func() {
		randNodeState = randNode.state
	})
	for randNodeState == Leader {
		idx = rand.Intn(len(raftNodes))
		randNode = raftNodes[idx]
		randNode.withLock("", func() {
			randNodeState = randNode.state
		})
	}

	// time.Sleep(300*time.Millisecond)
	log.Printf("[==tester==] Killing random follower node (%d)", idx)
	randNode.Kill()

	time.Sleep(2 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)

	log.Printf("[==tester==] Reviving random raft node (%d)", idx)
	randNode.Revive()

	time.Sleep(1 * time.Second)

	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
	time.Sleep(1 * time.Second)

	// selecting leader node
	var leaderNode *Raft
	for _, node := range raftNodes {
		node.withLock("", func() {
			if node.state == Leader {
				leaderNode = node
			}
		})
	}

	utils.Debug.Store(1)
	log.Printf("[==tester==] Killling leader raft node")
	leaderNode.Kill()

	time.Sleep(1 * time.Second)
	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)
	time.Sleep(1 * time.Second)

	// log.Printf("[==tester==] Reviving former leader raft node")
	// leaderNode.Revive()

	// time.Sleep(2 * time.Second)
	// checkLeaderElection(t, raftNodes)
	// checkTermEquality(t, raftNodes)
}

func testLogReplication(t *testing.T, raftNodes []*Raft, _ []string) {
	var term int

	rf := raftNodes[2]
	rf.withLock("", func() {
		term = rf.currentTerm
	})

	entries_1 := []map[string]interface{}{
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

	res, err := ClientSendData(entries_1)
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

	entries_2 := []map[string]interface{}{
		{
			"data": "this should be the fifth",
			"term": term,
		},
		{
			"data": "this should be the sixth",
			"term": term,
		},
	}

	res, err = ClientSendData(entries_2)
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

	var commitIndex int

	rf.withLock("", func() {
		commitIndex = rf.commitIndex
	})

	// ensuring randnode isn't a leader node
	var randNode *Raft
	var randNodeState State

	randNode = raftNodes[rand.Intn(len(raftNodes))]
	randNode.withLock("", func() {
		randNodeState = randNode.state
	})
	for randNodeState == Leader {
		randNode = raftNodes[rand.Intn(len(raftNodes))]
		randNode.withLock("", func() {
			randNodeState = randNode.state
		})
	}

	// randomly truncated incorrect log
	randNode.withLock("", func() {
		randNode.log = randNode.log[:2]
		randNode.commitIndex = 2
	})
	time.Sleep(2 * time.Second)

	panic("here")

	// Check for exactly 1 leader
	checkLeaderElection(t, raftNodes)
	// Check for equality of terms across nodes
	checkTermEquality(t, raftNodes)
	// Check for complete equality of logs across nodes
	checkLogConsistency(t, raftNodes)
	// Check for committed
	checkCommitted(t, raftNodes, commitIndex)

	////
	////
	////
	////
	////
	////
	////
	////
	////
	////
	////
	////

	// // ensuring randnode isn't a leader node
	// randNode = raftNodes[rand.Intn(len(raftNodes))]
	// randNode.withLock("", func(){
	// 	randNodeState = randNode.state
	// })

	// for randNodeState == Leader {
	// 	randNode = raftNodes[rand.Intn(len(raftNodes))]
	// 	randNode.withLock("", func(){
	// 		randNodeState = randNode.state
	// 	})
	// }

	// utils.Debug.Store(1)

	// randNode.Kill()
	// time.Sleep(2 * time.Second)

	// // Check for exactly 1 leader
	// checkLeaderElection(t, raftNodes)
	// // Check for equality of terms across nodes
	// checkTermEquality(t, raftNodes)
	// panic("here")
	// // Check for complete equality of logs across nodes
	// checkLogConsistency(t, raftNodes)
	// // Check for committed
	// checkCommitted(t, raftNodes, res.CommitIndex)

	// entries_3 := []map[string]interface{}{
	// 	{
	// 		"data": "this should be the seventh",
	// 		"term": term,
	// 	},
	// 	{
	// 		"data": "this should be the eight",
	// 		"term": term,
	// 	},
	// }

	// go func() {
	// 	utils.Debug.Store(1)
	// 	time.Sleep(time.Second * 5)
	// 	panic("timeout")
	// }()

	// res, err = ClientSendData(entries_3)
	// if err != nil {
	// 	t.Errorf("Error sending client request to Raft: %s", err)
	// }
	// time.Sleep(2 * time.Second)

	// panic("here uwuwuuw")

	// randNode.Revive()

	// time.Sleep(1 * time.Second)

	// Check for exactly 1 leader
	// checkLeaderElection(t, raftNodes)
	// // Check for equality of terms across nodes
	// checkTermEquality(t, raftNodes)
	// // Check for complete equality of logs across nodes
	// checkLogConsistency(t, raftNodes)
	// // Check for committed
	// checkCommitted(t, raftNodes, res.CommitIndex)

	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//

	// // selecting leader node
	// var leaderNode *Raft
	// for _, node := range raftNodes {
	// 	node.withLock("", func(){
	// 		if node.state == Leader {
	// 			leaderNode = node
	// 		}
	// 	})
	// }

	// leaderNode.Kill()
	// time.Sleep(1 * time.Second)

	// randNode.withLock("", func(){
	// 	term = randNode.currentTerm
	// })

	// entries_4 := []map[string]interface{}{
	// 	{
	// 		"data": "this should be the ninth",
	// 		"term": term,
	// 	},
	// 	{
	// 		"data": "this should be the tenth",
	// 		"term": term,
	// 	},
	// }

	// res, err = ClientSendData(entries_4)
	// if err != nil {
	// 	t.Errorf("Error sending client request to Raft: %s", err)
	// }

	// time.Sleep(1 * time.Second)

	// // Check for exactly 1 leader
	// checkLeaderElection(t, raftNodes)
	// // Check for equality of terms across nodes
	// checkTermEquality(t, raftNodes)
	// // Check for complete equality of logs across nodes
	// checkLogConsistency(t, raftNodes)
	// // Check for committed
	// checkCommitted(t, raftNodes, res.CommitIndex)

	// // go func() {
	// // 	time.Sleep(8 * time.Second)
	// // 	panic("timeout panic!!!")
	// // }()
	// // utils.Debug.Store(1)

	// time.Sleep(1 * time.Second)
	// leaderNode.Revive()
	// time.Sleep(1 * time.Second)
}

// helpers / unit tests

func checkLeaderElection(t *testing.T, raftNodes []*Raft) {
	t.Helper()
	var leaderCnt int = 0

	for _, rf := range raftNodes {
		var killed bool

		rf.withLock("", func() {
			if rf.killed() {
				killed = true
				return
			}

			if rf.state == Leader {
				leaderCnt += 1
			}
		})

		if killed {
			continue
		}
	}

	if leaderCnt != 1 {
		t.Errorf("Failed Leader Election; expected 1 leader, got %d", leaderCnt)
	}
}

func checkTermEquality(t *testing.T, raftNodes []*Raft) {
	t.Helper()

	var term int
	node := raftNodes[0]

	node.withLock("", func() {
		term = node.currentTerm
	})

	for _, rf := range raftNodes[1:] {
		var killed bool

		rf.withLock("", func() {
			if rf.killed() {
				killed = true
				return
			}

			if rf.currentTerm != term {
				t.Errorf("Failed Leader Election; node with inconsistent term found (%d = %d) (0 = %d)", rf.me, rf.currentTerm, term)
			}
		})

		if killed {
			continue
		}
	}
}

func checkLogConsistency(t *testing.T, raftNodes []*Raft) {
	t.Helper()

	var log []map[string]interface{}
	node := raftNodes[0]

	node.withLock("", func() {
		log = node.log
	})

	for _, node := range raftNodes[1:] {
		node.withLock("", func() {
			if !areEqual(log, node.log) {
				t.Errorf("logs differ across nodes; comparing nodes 0 and %d", node.me)
			}
		})
	}
}

func checkCommitted(t *testing.T, raftNodes []*Raft, commitIndex int) {
	t.Helper()

	for _, node := range raftNodes[:] {
		node.withLock("", func() {
			if node.commitIndex != commitIndex {
				t.Errorf(
					"commit index differs across nodes; comparing leader (%d) and %d (%d)",
					commitIndex,
					node.me,
					node.commitIndex,
				)
			}
		})
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
	time.Sleep(1 * time.Second)
	return raftNodes, peerAddresses
}
