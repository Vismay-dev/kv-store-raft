package kvservice

func main() {
	// ideally all of this should be abstracted away by config.go
	peerAddresses := []string{":8000", ":8001", ":8002", ":8003", ":8004"}
	kvServers := []*Server{}

	for i := range peerAddresses {
		kvServer := StartServer(peerAddresses, i)
		kvServers = append(kvServers, kvServer)
	}

	// for _, kvServer := range kvServers {

	// }
}
