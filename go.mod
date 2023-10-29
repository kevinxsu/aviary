module aviary

go 1.21.3

require (
	github.com/gocql/gocql v1.6.0
	go.uber.org/zap v1.26.0
)

require (
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	go.uber.org/multierr v1.10.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.11.1
