package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/utils"
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type Raft struct {
	mu              sync.Mutex
	state           State
	me              int
	currentTerm     int
	votedFor        int
	peers           []string
	timerChElection chan struct{}
	timerChHb       chan struct{}

	log         []map[string]interface{}
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	leaderId    int

	applyCh chan map[string]interface{}
	opCh    chan map[string]interface{}

	dead int32
}

func Make(peers []string, me int, applyCh, opCh chan map[string]interface{}) *Raft {
	rf := &Raft{
		state:           Follower,
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		peers:           peers,
		timerChElection: make(chan struct{}, 1),
		timerChHb:       make(chan struct{}, 1),

		log:         make([]map[string]interface{}, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   []int{},
		matchIndex:  []int{},
		applyCh:     applyCh,
		opCh:        opCh,

		dead: 0,
	}

	go rf.serve()

	return rf
}

func (rf *Raft) Start() {
	var rfState State
	rf.withLock("", func() {
		rfState = rf.state
		rf.readPersist()
	})

	go rf.electionTimeout()
	go rf.trackLastApplied()
	if rfState == Leader {
		go rf.sendHeartbeats()
		return
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) Revive() {
	if rf.killed() {
		rf.withLock("", func() {
			rf.timerChElection <- struct{}{}
		})
		atomic.StoreInt32(&rf.dead, 0)
	}
}

func (rf *Raft) killed() bool {
	return int(atomic.LoadInt32(&rf.dead)) == 1
}

func (rf *Raft) GetState() State {
	var state State
	rf.withLock("", func() {
		state = rf.state
	})
	return state
}

func (rf *Raft) serve() {
	var rfId int
	var peerAddr string
	rf.withLock("", func() {
		rfId = rf.me
		peerAddr = rf.peers[rf.me]
	})

	serviceName := fmt.Sprintf("Raft-%d", rfId)
	if err := rpc.RegisterName(serviceName, rf); err != nil {
		utils.Dprintf("Failed to register RPC: %s", err)
	}

	listener, err := net.Listen("tcp", peerAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %s", peerAddr, err)
	}

	go func() {
		defer func() {
			if err := listener.Close(); err != nil {
				log.Fatalf("Failed to close listener @ %s", listener.Addr().String())
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf("[%d @ %s] Failed to accept connection: %s", rfId, peerAddr, err)
			}
			go rpc.ServeConn(conn)
		}
	}()
}

func (rf *Raft) trackLastApplied() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var op map[string]interface{}
			rf.withLock("", func() {
				if rf.commitIndex > rf.lastApplied {
					op = rf.log[rf.lastApplied]
					rf.lastApplied++
				}
			})
			if op != nil {
				rf.applyCh <- op
				<-rf.opCh
			}
		}
	}
}

func (rf *Raft) withLock(_ string, f func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f()
}
