package kvservice

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vismaysur/kv-store-raft/internal/raft"
	"github.com/vismaysur/kv-store-raft/internal/utils"
)

type Server struct {
	mu sync.Mutex

	rf      *raft.Raft
	me      int
	address string
	store   map[string]string

	lastApplied map[int]int
	applyCh     chan map[string]interface{}
	opCh        chan map[string]interface{}
}

func StartServer(peerAddresses []string, me int) *Server {
	applyCh := make(chan map[string]interface{}, 1)
	opCh := make(chan map[string]interface{}, 1)

	kv := &Server{
		me:          me,
		store:       make(map[string]string),
		address:     peerAddresses[me],
		rf:          raft.Make(peerAddresses, me, applyCh, opCh),
		lastApplied: make(map[int]int),
		applyCh:     applyCh,
		opCh:        opCh,
	}

	kv.rf.Start()
	kv.serve()

	go kv.applyOpsLoop()

	return kv
}

func (kv *Server) Get(req *GetRequest, res *GetResponse) error {
	var value string
	var ok bool

	kv.withLock("", func() {
		value, ok = kv.store[req.Key]
	})

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
	var lastApplied int

	kv.withLock("", func() {
		lastApplied = kv.lastApplied[int(req.ClientId)]
	})

	if lastApplied >= int(req.ReqId) {
		return nil
	}

	entries := []map[string]interface{}{
		{
			"data": fmt.Sprintf(
				"key:%s|raft-delimiter|value:%s|raft-delimiter|op:%s|raft-delimiter|client:%d|raft-delimiter|request:%d|raft-delimiter|",
				req.Key,
				req.Value,
				req.Op,
				req.ClientId,
				req.ReqId),
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

	kv.waitForApply(req.ReqId, req.ClientId)

	return nil
}

func (kv *Server) waitForApply(requestId int32, clientId int32) {
	for {
		var condition bool

		kv.withLock("", func() {
			lastApplied, ok := kv.lastApplied[int(clientId)]
			if ok && lastApplied >= int(requestId) {
				kv.lastApplied[int(clientId)] = int(requestId)
				condition = true
			}
		})

		if condition {
			break
		}
	}
}

func (kv *Server) applyOpsLoop() {
	for {
		select {
		case entry := <-kv.applyCh:
			delimiter := "|raft-delimiter|"
			data := entry["data"].(string)

			parts := strings.Split(data, delimiter)
			if len(parts) != 6 {
				log.Fatal("log retrieved from node is of unknown format")
			}

			key := parts[0][4:]
			value := parts[1][6:]
			op := parts[2][3:]
			clientId, _ := strconv.Atoi(parts[3][7:])
			requestId, _ := strconv.Atoi(parts[4][8:])

			kv.withLock("", func() {
				if op == "Put" {
					kv.store[key] = value
				} else if op == "Append" {
					_, ok := kv.store[key]
					if !ok {
						kv.store[key] = value
					} else {
						kv.store[key] += value
					}
				}
				kv.opCh <- entry
				kv.lastApplied[clientId] = requestId
			})

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
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
