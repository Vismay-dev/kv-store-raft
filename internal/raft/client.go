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

func ClientSendData(entries []map[string]interface{}) error {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		return fmt.Errorf("error dialing IP address via TCP: %s", err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)

	args := &ClientReqRequest{
		Entries: entries,
	}
	var reply ClientReqResponse

	node := 0
	err = client.Call(fmt.Sprintf("Raft-%d.SendData", node), &args, &reply)
	for err != nil {
		node += 1
		if node > 4 {
			return err
		}
		err = client.Call(fmt.Sprintf("Raft-%d.SendData", node), &args, &reply)
	}

	return nil
}
