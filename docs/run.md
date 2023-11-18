# Running Aviary (IN PROGRESS)

Open up three separate terminals, cd to `aviary/main/`, and run one of the following commands in only one terminal.

```bash
$ go run aviarycoordinator.go
$ go run aviaryworker.go
$ go run clerk.go
```

NOTE: this is in-progress (there's no colocation between Aviary Workers and `mongod` shards yet).
