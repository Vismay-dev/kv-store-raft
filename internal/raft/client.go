package raft

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
)

func init() {
	gob.Register(map[string]interface{}{})
}

func ClientSendData(entries []map[string]interface{}) (*ClientReqResponse, error) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		return nil, fmt.Errorf("error dialing IP address via TCP: %s", err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)

	args := &ClientReqRequest{
		Entries: entries,
	}
	reply := &ClientReqResponse{}

	node := 0

	for ; ; node = node + 1 {
		err = client.Call(fmt.Sprintf("Raft-%d.SendData", node), &args, &reply)

		if err == nil && reply.Err == nil {
			break
		}
		if reply.Err != nil && reply.Err != ErrIncorrectLeader {
			return nil, reply.Err
		}
	}

	return reply, nil
}
