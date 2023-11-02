# TODO

## Docker
- [ ] Figure out how to setup sharded MongDB clusters on Docker (mongos + config server + mongod)
- [ ] Figure out how communication between containerized nodes work (probably just use local port and have containers communicate through there)
- [ ] Figure out how to write a docker-compose file/Dockerfile (to make it easier to setup the distributed environment)

## Design 
- [ ] Finalize Aviary's high-level architecture 
    - [ ] Determine what requirements the underlying database must have
    - [ ] Figure out how we're going to do MapReduce
    - [ ] Figure out how to use MongoDB's MVCC for colocated Aviary workers to directly communicate with mongod shards (this is pretty illegal according to mongodb docs though)
- [ ] How to make Aviary coordinator fault tolerant?
- [ ] How does client query Aviary coordinator?

## Programming
- [ ] Read MongoDB Go driver
- [ ] Aviary Coordinator 
- [ ] Aviary Worker 
    - for now, just have a single worker in each container co-located with the shard 
    - in future, can have additional process that spawns workers (k8s type)

## Papers 
- [ ] MapReduce
- [ ] https://www.cs.princeton.edu/~wlloyd/papers/port-osdi20.pdf
