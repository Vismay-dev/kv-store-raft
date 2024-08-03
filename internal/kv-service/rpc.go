package kvservice

import (
	"log"
	"net"
	"net/rpc"

	"github.com/vismaysur/kv-store-raft/internal/utils"
)

type GetRequest struct {
	Key      string
	ReqId    int32
	ClientId int32
}

type GetResponse struct {
	Value string
	Err   string
}

type PutAppendRequest struct {
	Key      string
	Value    string
	Op       string
	ReqId    int32
	ClientId int32
}

type PutAppendResponse struct {
	Err string
}

func call(peer, rpcname string, req interface{}, res interface{}) bool {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		utils.Dprintf("Failed to dial %s: %s", peer, err)
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)

	if err := client.Call(rpcname, req, res); err != nil {
		log.Printf(
			"[Clerk] RPC call to [%s] failed: %s, %v",
			peer,
			err,
			res,
		)
		return false
	}

	return true
}
