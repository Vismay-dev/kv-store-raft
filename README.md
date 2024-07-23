## kv-store-raft

![Go CI](https://github.com/vismaysur/kv-store-raft/actions/workflows/go-test.yml/badge.svg)

Scalable and fault-tolerant distributed key-value store implementing the Raft consensus protocol for strong consistency. Based on the [Raft Consensus Algorithm](http://nil.lcs.mit.edu/6.824/2020/papers/raft-extended.pdf) extended paper by Diego Ongaro and John Ousterhout.

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

### Note

- While this project is heavily inspired by the MIT 6.8240 labs, it does not borrow/use any code or design choices from an implementation by any faculty/student involved with the course.
