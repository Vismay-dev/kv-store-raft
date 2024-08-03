package kvservice

import (
	"fmt"
	"net/rpc"
	"sync"

	"github.com/vismaysur/kv-store-raft/internal/raft"
	"github.com/vismaysur/kv-store-raft/internal/utils"
)

type Server struct {
	mu sync.Mutex

	rf      *raft.Raft
	me      int
	address string
	store   map[string]string
}

func StartServer(peerAddresses []string, me int) *Server {
	kv := &Server{
		me:      me,
		store:   make(map[string]string),
		address: peerAddresses[me],
		rf:      raft.Make(peerAddresses, me),
	}

	kv.rf.Start()
	kv.serve()

	return kv
}

func (kv *Server) Get(req *GetRequest, res *GetResponse) error {
	value, ok := kv.store[req.Key]

	if kv.rf.GetState() != raft.Leader {
		res.Err = raft.ErrIncorrectLeaderStr
		return nil
	}

	if !ok {
		res.Value = ""
	}
	res.Value = value

	return nil
}

func (kv *Server) PutAppend(req *PutAppendRequest, res *PutAppendResponse) error {
	_, ok := kv.store[req.Key]

	entries := []map[string]interface{}{
		{
			"data": fmt.Sprintf("key:%s value:%s op:%s", req.Key, req.Value, req.Op),
			"term": nil,
		},
	}

	if err := kv.rf.SendDataLocal(entries); err != nil {
		if err == raft.ErrDeadNode {
			res.Err = raft.ErrDeadNodeStr
			return nil
		} else if err == raft.ErrIncorrectLeader {
			res.Err = raft.ErrIncorrectLeaderStr
			return nil
		}
		return err
	}

	if !ok || req.Op == "Put" {
		kv.store[req.Key] = req.Value
	} else {
		kv.store[req.Key] += req.Value
	}

	return nil
}

func (kv *Server) serve() {
	var kvId int

	kv.withLock("", func() {
		kvId = kv.me
	})

	serviceName := fmt.Sprintf("KVStore-%d", kvId)
	if err := rpc.RegisterName(serviceName, kv); err != nil {
		utils.Dprintf("Failed to register RPC: %s", err)
	}
}

func (kv *Server) withLock(_ string, f func()) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	f()
}
