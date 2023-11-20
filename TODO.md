# TODO

## Docker

- [x] Figure out how to setup sharded MongoDB clusters on Docker (mongos + config server + mongod)
- [x] Figure out how communication between containerized nodes work (probably just use local port and have containers communicate through there)
- [x] Write a docker-compose file
- [ ] Install Go in the MongoDB Docker images so we can just directly run the Workers there

## Design

- [x] Finalize Aviary's high-level architecture
  - [x] Determine what requirements the underlying database must have
  - [x] Figure out how we're going to do MapReduce
  - [x] Figure out how to use MongoDB's MVCC for colocated Aviary workers to directly communicate with mongod shards (this is pretty illegal according to mongodb docs though)
- [x] How to make Aviary coordinator fault tolerant?
  - still in progress
- [x] How does client query Aviary coordinator?
- [x] Aviary Coordinator
- [x] Aviary Worker
  - [ ] for now, just have a single worker in each container co-located with the shard (still need to do this)

## Implementation

- [] Define MAP and REDUCE function API (i.e. 6.5840 style)
- [] Implement MongoDB communication for Workers
- [] Setup multiple Workers with one Coordinator (currently just one non-colocated worker, coordinator, and clerk that all query mongos instead of directly with shard)
- [] Make RPC and general structs have better design (currently a bunch of spaghetti)
  - e.g. Job struct is pretty poorly thought out: How should Jobs vs. Tasks be represented/implemented?
  - Should each Worker be able to spawn more goroutines to execute MAP/REDUCE functions? How would this work?
- [] Figure out how to have Workers execute the MAP/REDUCE functions
  - have clients send map.so/reduce.so to Clerk -> convert to []byte and insert that into MongoDB?
  - need to figure out how the Go runtime works with *.so
- [] Implement persistence for Coordinator and Workers
- [] Implement fault-tolerance for the Workers and the Coordinator
  - should their state be stored into MongoDB, or use something like Raft?
- [] FIGURE OUT HOW TO SETUP MULTIPLE WORKERS
  - [] Implement a Pub/Sub messaging scheme so any new workers can listen in the Coordinator gets a new client request?
  - [] or just hardcode a list of ports that each worker will be on??
  - [] also need to think of how workers should be set up
- [] Figure out how intermediate files/data from the MAP phase should be stored
  - could either insert directly into mongodb, save into the container using os.Write(), or keep it in memory (all three seem slow)
- [] Figure out how intermediate data shuffle should be done
  - could try to leverage MongoDB's sharding policies and shard balancer, but need to first better understand MongoDB sharding
- [] Run an actual MapReduce job to show proof-of-concept
- [] Refactor Coordinator to have Client Contexts (to maintain db connection)

## BUGS

- [] segfault in aviaryworker when clerk gets `d` 3 times in a row for multiple workers
- [] even if worker dies, coordinator still might try to send a task to it (need to address this)
