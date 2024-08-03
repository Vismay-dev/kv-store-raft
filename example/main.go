package main

import (
	"fmt"
	"log"
	"os"
	"path"

	kvservice "github.com/vismaysur/kv-store-raft/internal/kv-service"
)

func main() {
	// create a temporary directory for this demo (change this as you require)
	cwd, _ := os.Getwd()
	storagePath := path.Join(cwd, "/server_store")
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatal(err)
	}

	// example code:
	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	clerk := kvservice.StartServers(peerAddresses, storagePath)

	key := "k1"
	value := "v1"
	clerk.Put(key, value)

	out, _ := clerk.Get(key)
	fmt.Println(out)

	additionalValue := "->v2"
	_ = clerk.Append(key, additionalValue)

	out, _ = clerk.Get(key)
	fmt.Println(out)

	// play around!
}
