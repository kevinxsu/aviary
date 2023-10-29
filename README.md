# aviary

## Setting up a ScyllaDB three-node cluster (from example code)

```
$ git clone https://github.com/scylladb/scylla-code-samples.git
$ echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf
$ sysctl -p /etc/sysctl.conf
$ docker stop $(docker ps -aq)
$ docker rm $(docker ps -aq)
$ cd scylla-code-samples/mms
$ docker compose up -d
```
After a minute or so, the nodes should be up and running and can be verified with 
```
$ docker exec -it scylla-node1 nodetool status
```
