## kv-store-raft

![Go CI](https://github.com/vismaysur/kv-store-raft/actions/workflows/go-test.yml/badge.svg)

Scalable and fault-tolerant distributed key-value store implementing the Raft consensus protocol for strong consistency. Based on the [Raft Consensus Algorithm](http://nil.lcs.mit.edu/6.824/2020/papers/raft-extended.pdf) extended paper by Diego Ongaro and John Ousterhout.

### Integration Flow / Architecture

```mermaid
sequenceDiagram
    participant Client
    participant KVStore Service
    participant Raft Module
    participant RaftPeer Service
    participant Other Nodes

    Client->>KVStore Service: Put/Delete Request
    KVStore Service->>Raft Module: Is Leader?
    alt Is Leader
        Raft Module-->>KVStore Service: Yes
        KVStore Service->>Raft Module: Propose Log Entry
        Raft Module->>RaftPeer Service: AppendEntries RPC
        RaftPeer Service->>Other Nodes: Replicate Log
        Other Nodes-->>RaftPeer Service: Acknowledge
        RaftPeer Service-->>Raft Module: Log Committed
        Raft Module->>KVStore Service: Apply Command
        KVStore Service-->>Client: Operation Result
    else Not Leader
        Raft Module-->>KVStore Service: No
        KVStore Service-->>Client: Redirect to Leader
    end

    Client->>KVStore Service: Get Request
    KVStore Service->>KVStore Service: Read Local State
    KVStore Service-->>Client: Value
```

### Test Command

To run tests, use the following command:

```sh
make test
```

### Additional References

- [MIT 6.8240 Spring 20'](https://www.youtube.com/watch?v=64Zp3tzNbpE&list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB&index=7)
- [ZooKeeper](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf)
- [CRAQ](https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf)
- [Aurora DB - Cloud Replicated DB](https://pages.cs.wisc.edu/~yxy/cs764-f20/papers/aurora-sigmod-17.pdf)
- [Fringipani - Cache Consistency](https://pdos.csail.mit.edu/6.824/papers/thekkath-frangipani.pdf)
