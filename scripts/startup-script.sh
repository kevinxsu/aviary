docker rmi $(docker images -f "dangling=true" -q)
docker network inspect test-net >/dev/null 2>&1 || docker network create -d bridge test-net
docker network ls
docker container ls
docker-compose up -d
docker run --network test-net
# https://forum.scylladb.com/t/i-want-to-setup-scylladb-on-docker-in-my-local-environment-and-then-i-want-to-connect-it-to-the-node-i-tried-but-connection-is-not-established/683 
