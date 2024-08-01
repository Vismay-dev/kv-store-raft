package kvservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/vismaysur/kv-store-raft/internal/raft"
	"github.com/vismaysur/kv-store-raft/internal/raft/utils"
)

type Server struct {
	mu sync.Mutex

	rf *raft.Raft
	me int
}

func StartServer(peerAddresses []string, me int) *Server {
	kv := &Server{
		me: me,
	}

	kv.rf = raft.Make(peerAddresses, me)
	kv.rf.Start()

	return kv
}

func (kv *Server) server() {
	var kvId int
	var peerAddr string
	kv.withLock("", func() {
		kvId = kv.me
		peerAddr = kv.peers[rf.me]
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
