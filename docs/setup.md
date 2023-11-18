# Setting up Aviary Environment on Docker

We use Docker to simulate a distributed MongoDB database.

## Steps to Start Up a Sharded MongoDB Cluster

0. Install `docker` and `docker-compose`.
1. Start the containers
```bash
$ docker-compose up -d
```
2. Initialize the config server and shard replica sets
```bash
docker-compose exec configsvr01 sh -c "mongosh < /scripts/init-configserver.js"
docker-compose exec shard01-a sh -c "mongosh < /scripts/init-shard01.js"
docker-compose exec shard02-a sh -c "mongosh < /scripts/init-shard02.js"
docker-compose exec shard03-a sh -c "mongosh < /scripts/init-shard03.js"
```
3. Wait a bit for the replica sets to elect primary.
4. Initialize the router
```bash
docker-compose exec router01 sh -c "mongosh < /scripts/init-router.js"
```
5. Enable sharding and setup the shard keys
```bash
docker-compose exec router01 mongosh --port 27017
sh.enableSharding("db")
db.adminCommand({shardCollection: "db.coll", key: {oemNumber: "hashed", zipCode: 1, supplierId: 1}})
```
<!-- sh.enableSharding("MyDatabase") -->
<!-- db.adminCommand({shardCollection: "MyDatabase.MyCollection", key: {oemNumber: "hashed", zipCode: 1, supplierId: 1}}) -->

The MongoDB URI connection string is `mongodb://127.0.0.1:27117,127.0.0.1:27118`, which can be used with the MongoDB Go driver to query the database.

## Checking Status

```bash
# enter mongoshell
docker exec -it router-01 mongosh

# then inside mongoshell, enter the following commands
[direct: mongos] user> use db
[direct: mongos] db> db.stats()
[direct: mongos] db> db.coll.getShardDistribution()
```
<!-- [direct: mongos] user> use MyDatabase -->
<!-- [direct: mongos] MyDatabase> db.stats() -->
<!-- [direct: mongos] MyDatabase> db.MyCollection.getShardDistribution() -->

```bash
# sharded cluster status
docker exec -it router-01 mongosh

# then in mongosh, enter the following command
[direct: mongos] user> sh.status()
```

```bash
# replica set status for shards
docker exec -it shard-01-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
docker exec -it shard-02-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
docker exec -it shard-03-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
```

```bash
# other similar status commands
docker exec -it mongo-config-01 bash -c "echo 'rs.status()' | mongosh --port 27017"
docker exec -it shard-01-node-a bash -c "echo 'rs.help()' | mongosh --port 27017"
docker exec -it shard-01-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
docker exec -it shard-01-node-a bash -c "echo 'rs.printReplicationInfo()' | mongosh --port 27017" 
docker exec -it shard-01-node-a bash -c "echo 'rs.printSlaveReplicationInfo()' | mongosh --port 27017"
```

## Startup and Shutdown

The cluster only has to be initialized on the first startup.
Future startups only require `docker-compose up` or `docker-compose up -d`.
To shut down the cluster environments, go to the directory where the docker-compose.yml file is and run `docker-compose down`.
To reset the cluster (this removes data), stop all containers and run `docker-compose rm`.
To clean up docker-compose, run `docker-compose down -v -rmi all --remove-orphans`.