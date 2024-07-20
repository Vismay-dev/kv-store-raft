# server store (makeshift persistence)
SERVER_STORE := ./server_store

.PHONY: clean
clean: 
	rm -rf ${SERVER_STORE}

.PHONY: test
test: clean
	mkdir -p ${SERVER_STORE}
	go test -count=1 -v -race ./...