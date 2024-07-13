package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

type TestCase func(t *testing.T, raftNodes []*Raft, peerAddresses []string)

// functional tests

func TestLeaderElection(t *testing.T) {
	raftNodes, peerAddrs := setup(t)
	for testName, testFunc := range map[string]TestCase{
		"TestNormal":           testNormal,
		"TestNetworkPartition": testNetworkPartition,
	} {
		t.Run(testName, func(t *testing.T) {
			testFunc(t, raftNodes, peerAddrs)
		})
	}
}

// integration tests

func testNormal(t *testing.T, raftNodes []*Raft, peerAddrs []string) {
	// Check for exactly 1 leader
	checkLeaderElection(t, raftNodes)
	// Check for equality of terms across nodes
	checkTermEquality(t, raftNodes)
}

func testNetworkPartition(t *testing.T, raftNodes []*Raft, peerAddrs []string) {
	randNode := raftNodes[rand.Intn(len(raftNodes))]

	log.Printf("[==tester==] Killing raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	randNode.Kill()
	time.Sleep(2 * time.Second)

	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)

	log.Printf("[==tester==] Reviving raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	randNode.Revive()
	time.Sleep(2 * time.Second)

	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)

	anotherRandNode := raftNodes[rand.Intn(len(raftNodes))]

	log.Printf("[==tester==] Killing raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	randNode.Kill()
	log.Printf("[==tester==] Killing raft node (%d @ %s)", anotherRandNode.me, peerAddrs[anotherRandNode.me])
	anotherRandNode.Kill()
	time.Sleep(2 * time.Second)

	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)

	log.Printf("[==tester==] Reviving raft node (%d @ %s)", randNode.me, peerAddrs[randNode.me])
	randNode.Revive()
	time.Sleep(2 * time.Second)

	checkLeaderElection(t, raftNodes)
	checkTermEquality(t, raftNodes)

	log.Printf("[==tester==] Reviving raft node (%d @ %s)", anotherRandNode.me, peerAddrs[anotherRandNode.me])
	anotherRandNode.Revive()
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
		if rf.currentTerm != term {
			t.Errorf("Failed Leader Election; node with inconsistent term found (%d)", rf.me)
		}
		rf.mu.Unlock()
	}
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
