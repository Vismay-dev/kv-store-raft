package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestLeaderElection(t *testing.T) {
	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	raftNodes := []*Raft{}

	for i, _ := range peerAddresses {
		rf := Make(peerAddresses, i)
		raftNodes = append(raftNodes, rf)
	}

	fmt.Printf("All nodes initialized...\n")

	for _, rf := range raftNodes {
		go rf.electionTimeout()
	}

	time.Sleep(5 * time.Second)
}
