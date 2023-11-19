# Aviary Design

At a high level, Aviary is a wrapper around MongoDB that allows for MapReduce tasks, acting as a serverless computing framework for compute-heavy workloads on the database. (Isn't this just OLAP??...)

## MapReduce in Aviary

The main difference between MapReduce in Aviary versus other popular offerings (e.g. Hadoop) is that Aviary treats the database itself as the distributed file system. Hadoop MapReduce uses HDFS (Hadoop File System), which is a distributed file system designed for storing big data. Using MongoDB as the underlying database for Aviary is quite nice, since MongoDB provides MVCC and distributed transactions, which essentially gives us built-in fault-tolerance.

- actually, this might not be true since we're not storing state into the database itself (but we could...?)

Aviary relies on the Clerk, which is teh fronten through which clients can start their MapReduce jobs, the Aviary Coordinator (AvCoordinator), which is the backend that handles worker coordination and job scheduling/management, and the Aviary Workers (AvWorker), which are the processes that are co-located with each `mongod` shard server that actually performs the computation.

## MongoDB Crash Course

### Basic Terminology

- Tables are to RDBMS's as Collections are to MongoDB
- Rows are to RDBMS's as Documents are to MongoDB
- Columns are to RDBMS's as Fields are to MongoDB

### Sharded Clusters in MongoDB

A sharded MongoDB cluster has four main components:

  1. the config servers, which store metadata and configuration settings for the sharded cluster.
  2. the `mongos` query router, which provides an interface between client applications and the sharded cluster.
  3. the `mongod` shards themselves, which store the (sharded) data.
  4. the background shard balancer, which automatically balances and migrates shards
      - the balancing procedure is entirely transparent to the user/application layer
  
The config servers and the `mongod` shards are deployed as **replica sets**, which are groups of `mongod` processes that maintain the same data set, providing redundancy and high availability in the case of server crashes.

Data in MongoDB is sharded at the collection level, which distributes the collection data across the shards in the cluster. Furthermore, MongoDB partitions sharded data into **chunks**, which are contiguous ranges of shard key values within a particular shard (inclusive lower bound, exclusive upper bound)

- chunks are split when they grow beyond the configured chunk size, which by default is 128 megabytes
- chunks are migrated when a shard contains too many chunks of a collection relative to other shards

## Workflow

1. Client wants to start a MapReduce job.
    1. `go run clerk.go`
    2. `(aviary) ~> insert [path/to/map.go] [path/to/reduce.go]`
        - clerk will read in the files, convert them to strings, marshal into BSON, and insert to MongoDB
    3. `(aviary) ~> begin [client id] [map.go] [reduce.go] [database name] [collection name]`
    4. Wait for result, or see progress via `(aviary) ~> show [client id]`
2. Clerk sends RPCs to notify Coordinator of the new job
3. Coordinator does bookkeeping (what kind though??) and notifies the Workers through RPC.
    - TODO: implement observer pattern for server join/leave
4. Worker notifies Coordinator when done / Coordinator occasionally pings Worker to check progress
5. When MR job complete, results of MR job will be in the database. Client will not know whether or not it exists.
    - TODO: should the results only be accessible through Aviary?

## Aviary Coordinator

The Aviary Coordinator acts as a wrapper around the MongoDB database.
When clients want to start a MapReduce job over some range of data in their database, they must forward `Map()` and `Reduce()` functions (written in Go) to the Coordinator.

Aviary will create two additional collections within the client's database (we are not sure if this is a dealbreaker), called `aviary-funcs` and `aviary-intermediates`.
Each collection respectively stores the `Map()`/`Reduce()` functions, and the intermediate data from the various MAP and REDUCE phases.
The `aviary-funcs` collection **is not sharded** so that all AvWorkers can lookup the relevant functions.
The `aviary-intermediates` collection **is sharded** so that each worker can directly write to its local shard, bypassing the network for performance.

- again, not 100% sure if its okay to not go through a mongos router : really need to figure out if this will be okay or not

Once the functions have been inserted into the collection, the AvCoordinator will start notify and delegate tasks to AvWorkers, marking the start of a MapReduce job.

### Aviary Coordinator Functionality

#### Orchestrating and Scheduling MapReduce Tasks

The coordinator orchestrates and schedules client MapReduce requests. The coordinator will cryptographically generate unique task ID's corresponding to the new MapReduce task and sends this to the workers. This allows AvWorkers to store intermediate results of computation that could possible be re-used or queried at a future time with low probability of collision with irrelevant data. Or if so wished, all intermediate computations could be deleted once the associated MR job is complete.

- would be interesting to see if we could incorporate techniques from AWS On-demand Container Loading to cache results of intermediate computations (but this would be hard to do since client queries are likely to change data)
- does this require the client to upload their data in a specialized way? or would the client only need to specify which table and collection they want to MR over?

#### Saving Intermediates from the Map Phase

Since we are trying to maximize the usage of fault-tolerance that MongoDB provides by default, we elect to create a new collection in the client's database specifically for Aviary MapReduce jobs. The state of each MapReduce task can be totally determined by the data stored in this collection. For example, after starting some MapReduce tasks through Aviary, the database might look something like this:

```mongosh
  [mongos] MyDatabase> show dbs
  MyDatabase   44.17 MiB
  admin        80.00 KiB
  config        6.09 MiB
  test        156.00 KiB

  [mongos] MyDatabase> show collections
  MyCollection
  aviary
```

TODO: not yet decided whether or not it the `aviary` collection should be sharded as well, though thinking about it now, it might be worth doing since then the local AvWorkers can directly write to the primary shard
    - but if the AvWorkers directly write to the primary shard, will the balancer fuck things up?
    - need to think a bit harder about this

#### The Shuffle

TODO: Not sure how the shuffle should be done, but for now, probably just do it in a really really dumb way and just get all the relevant data through mongos? 

- alternatively, figure out a deterministic scheme for workers to reshard `aviary-intermediates` so that instead of directly writing to their local shard, they can just send everything through `mongos` and have the shuffle be "automatically" performed (not sure if this is possible)

### Aviary Coordinator External API

These are the outwards-facing interfaces that clients should call. We assume that the client's data is already stored in the database, and that it is unnecessary for the client to send input data to Aviary. If not, the client can simply just insert data into the database like they normally would.

```go
// asynchronously start a MapReduce job
bool StartMapReduce(tag string, _map, _reduce)

// query in-progress MapReduce jobs
[]string MapReduceJobs()
```

### Aviary Coordinator Internal API

These are the internal functions that are called in `main.go`.

### TODO

1. Determine whether or not users should forward all arguments through the Coordinator
    - probably not, just when a MapReduce task starts
    - but that would make real-time updates difficult since the Workers wouldn't know whether or not they're reading fresh/stale data?
2. Figure out how/where to store the result of the MapReduce task
    - probs just create a new collection to store temp results into
3. Figure out how to encode MR tasks uniquely
4. Figure out if it'll be okay to directly write to primary shards from a AvWorker

## Worker

The Aviary Worker is co-located with each MongoDB `mongod` shard instance. MongoDB's sharding scheme requires each shard to be in a replica set of three servers, which means that if the database has three shards, then there will be 9 total shard servers. A group of three `mongod` servers form one replica set for one shard -- servers here correspond to Docker containers. One Aviary Worker process lives within each of those containers.

### Aviary Worker Internal API

Clients should never be directly interacting with Aviary Workers, so all requests should go through the Aviary Coordinator first.

```go
// 
```


### WORKER TODO

Add fault-tolerance later, just get a working thing running on a single container first.
    - i think that AvWorkers should manage fault-tolerance here?
    - but coordinator also needs to keep track of what is happening too, but it doesn't make sense
        for the coordinator to manage worker fault-tolerance since they're running on a separate container

## Network and Communication

For the purposes of this project, we elect to simply setup networking within Docker itself and have Aviary processes communicate over gRPC through localhost, rather than implement a dedicated communication service (e.g. using a REST API or HTTP) between the coordinator and the workers. MongoDB shard clusters communicate through Docker as well (Docker bridge-local).

TODO: figure out how to have coordinator and worker communicate with each other through RPC in docker containers.

## Future Work

There are many things that can be improved on.

1. Using Kubernetes for container orchestration.
2. Using a dedicated networking communication scheme.
3. To make this more "serverless", we can incorporate the idea of on-demand container loading (AWS Lambda, Firecracker). Instead of having only a single Aviary Worker on each container, we can instead have an Aviary "Manager" that spawns multiple Aviary Workers, to increase concurrency during both the Map and the Reduce phase.
    - this would also open up multi-tenancy and enable concurrent MapReduce jobs.

TO THINK ABOUT: should fault-tolerance be done by storing coordinator (and worker??) state into the database?

TO THINK ABOUT: what happens if clients change data while a MR job is ongoing?
