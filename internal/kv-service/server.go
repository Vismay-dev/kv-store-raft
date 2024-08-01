package kvservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/vismaysur/kv-store-raft/internal/utils"
)

type Server struct {
	mu sync.Mutex

	// rf		*raft.Raft
	me      int
	address string
	store   map[string]string
}

func StartServer(peerAddresses []string, me int) *Server {
	kv := &Server{
		me:      me,
		store:   make(map[string]string),
		address: peerAddresses[me],
	}

	kv.serve()

	return kv
}

func (kv *Server) Get(req *GetRequest, res *GetResponse) error {
	value, ok := kv.store[req.Key]

	if !ok {
		res.Value = ""
	}
	res.Value = value

	return nil
}

func (kv *Server) PutAppend(req *PutAppendRequest, res *PutAppendResponse) error {
	_, ok := kv.store[req.Key]

	if !ok || req.Op == "Put" {
		kv.store[req.Key] = req.Value
	} else {
		kv.store[req.Key] += req.Value
	}

	return nil
}

func (kv *Server) serve() {
	var kvId int
	var addr string

	kv.withLock("", func() {
		kvId = kv.me
		addr = kv.address
	})

	serviceName := fmt.Sprintf("KVStore-%d", kvId)
	if err := rpc.RegisterName(serviceName, kv); err != nil {
		utils.Dprintf("Failed to register RPC: %s", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %s", addr, err)
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
				log.Fatalf("[%d @ %s] Failed to accept connection: %s", kvId, addr, err)
			}
			go rpc.ServeConn(conn)
		}
	}()
}

func (kv *Server) withLock(_ string, f func()) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	f()
}
