package main

import (
	"fmt"

	kvservice "github.com/vismaysur/kv-store-raft/internal/kv-service"
)

func main() {
	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	clerk := kvservice.StartServers(peerAddresses)

	key := "k1"
	value := "v1"
	clerk.Put(key, value)

	out, _ := clerk.Get(key)
	fmt.Print(out)

	additionalValue := "v2"
	_ = clerk.Append(key, additionalValue)

	out, _ = clerk.Get(key)
	fmt.Print(out)
}
