# server store (makeshift persistence)
SERVER_STORE := ./server_store

.PHONY: clean
clean: 
	rm -rf ${SERVER_STORE}

.PHONY: test-raft
test-raft: clean
	mkdir -p ${SERVER_STORE}
	go test -count=1 -v -race github.com/vismaysur/kv-store-raft/internal/raft

.PHONY: test-kvstore
test-kvstore: clean
	mkdir -p ${SERVER_STORE}
	go test -count=1 -v -race github.com/vismaysur/kv-store-raft/internal/kv-service