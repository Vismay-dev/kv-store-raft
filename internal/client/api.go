package client

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/vismaysur/kv-store-raft/internal/raft"
)

func SendData(entry *raft.LogEntry) error {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		return fmt.Errorf("error dialing IP address via TCP: %s", err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)

	args := &raft.ClientReqRequest{
		Entry: *entry,
	}
	var reply raft.ClientReqResponse

	if err := client.Call("Raft-0.SendData", &args, &reply); err != nil {
		return err
	}
	return nil
}
