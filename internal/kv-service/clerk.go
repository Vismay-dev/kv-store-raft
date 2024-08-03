package kvservice

import (
	"fmt"
	"sync/atomic"

	"github.com/vismaysur/kv-store-raft/internal/raft"
)

type Clerk struct {
	servers   []*Server
	clientId  int32
	leaderId  int32
	requestId int32
}

func MakeClerk(servers []*Server, clientId int32) *Clerk {
	ck := &Clerk{
		servers:   servers,
		clientId:  clientId,
		leaderId:  0,
		requestId: 0,
	}

	return ck
}

func (ck *Clerk) Get(key string) (string, error) {
	// atomic.AddInt32(&ck.requestId, 1)

	args := &GetRequest{
		Key:      key,
		ReqId:    ck.requestId,
		ClientId: ck.clientId,
	}

	server := ck.leaderId
	var value string
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := &GetResponse{}
		rpcname := fmt.Sprintf("KVStore-%d.Get", server)

		ok := call(ck.servers[server].address, rpcname, args, reply)

		if ok && reply.Err == "" {
			ck.leaderId = int32(server)
			value = reply.Value
			break
		}

		if !ok &&
			reply.Err != raft.ErrIncorrectLeaderStr &&
			reply.Err != raft.ErrDeadNodeStr {
			return "", fmt.Errorf("error occurred; could not serve request")
		}
	}

	return value, nil
}

func (ck *Clerk) Put(key string, value string) error {
	atomic.AddInt32(&ck.requestId, 1)
	leaderId := atomic.LoadInt32(&ck.leaderId)

	args := &PutAppendRequest{
		Key:      key,
		Value:    value,
		Op:       "Put",
		ReqId:    ck.requestId,
		ClientId: ck.clientId,
	}

	server := leaderId
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := &PutAppendResponse{}
		rpcname := fmt.Sprintf("KVStore-%d.PutAppend", server)

		ok := call(ck.servers[server].address, rpcname, args, reply)

		if ok && reply.Err == "" {
			ck.leaderId = int32(server)
			break
		}

		if !ok &&
			reply.Err != raft.ErrIncorrectLeaderStr &&
			reply.Err != raft.ErrDeadNodeStr {
			return fmt.Errorf("error occurred; could not serve request")
		}
	}

	return nil
}

func (ck *Clerk) Append(key string, arg string) error {
	atomic.AddInt32(&ck.requestId, 1)
	leaderId := atomic.LoadInt32(&ck.leaderId)

	args := &PutAppendRequest{
		Key:      key,
		Value:    arg,
		Op:       "Append",
		ReqId:    ck.requestId,
		ClientId: ck.clientId,
	}

	server := leaderId
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := &PutAppendResponse{}
		rpcname := fmt.Sprintf("KVStore-%d.PutAppend", server)

		ok := call(ck.servers[server].address, rpcname, args, reply)

		if ok && reply.Err == "" {
			ck.leaderId = int32(server)
			break
		}

		if !ok &&
			reply.Err != raft.ErrIncorrectLeaderStr &&
			reply.Err != raft.ErrDeadNodeStr {
			return fmt.Errorf("error occurred; could not serve request")
		}
	}

	return nil
}
