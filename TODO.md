# TODO

## Docker
- [ ] Figure out how to setup ScyllaDB clusters on Docker
- [ ] Figure out how communication between containerized nodes work
- [ ] Figure out how to write a docker-compose file/Dockerfile

## ScyllaDB
- [ ] Figure out how scylla.yaml works 
- [ ] Read more about Scylla's snapshots and sharding works
- [ ] Learn more about Scylla's architecture
    - [ ] consistency model (snapshot/mvcc?)
    - [ ] sharding
    - [ ] CQL vs Alternator modes

## Design 
- [ ] Finalize Aviary's high-level architecture 
    - [ ] Determine what requirements the underlying database must have
    - [ ] Figure out how we're going to do MapReduce and how 
    - [ ] Figure out if Scylla is good enough or if we should go back to MongoDB

## Programming
- [ ] Read `scylladb/gocql` driver documentation 
    - [ ] shard-awareness
- [ ] Look into `scylladb/cpp-driver`

## Papers 
- [ ] MapReduce
- [ ] https://www.cs.princeton.edu/~wlloyd/papers/port-osdi20.pdf
