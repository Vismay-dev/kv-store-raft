package kvservice

import (
	"log"
	"testing"
	"time"
)

func TestKVStore(t *testing.T) {
	// ideally all of this should be abstracted away by config.go
	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	kvServers := []*Server{}

	for i := range peerAddresses {
		kvServer := StartServer(peerAddresses, i)
		kvServers = append(kvServers, kvServer)
	}

	time.Sleep(2 * time.Second)

	clerk := MakeClerk(kvServers, 0)

	clerk.Put("key1", "some useful text")
	value, _ := clerk.Get("key1")
	log.Printf("%s\n", value)

	clerk.Put("key1", "overwritten useful text")
	value, _ = clerk.Get("key1")
	log.Printf("%s\n", value)

	clerk.Put("key2", "some useful text 2")
	value, _ = clerk.Get("key2")
	log.Printf("%s\n", value)

	clerk.Append("key2", " -- useful extension/appended text")
	value, _ = clerk.Get("key2")
	log.Printf("%s\n", value)

	clerk.Append("key3", "append text used as put instead")
	value, _ = clerk.Get("key3")
	log.Printf("%s\n", value)
}
