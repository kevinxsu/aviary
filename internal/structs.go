package aviary

import (
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type JobState = string

const (
	PENDING  JobState = "PENDING" // coordinator needs to partition the keys?
	READY             = "READY"   // coordinator is ready to give the job to the workers
	MAPPING           = "MAPPING"
	REDUCING          = "REDUCING"
	DONE              = "DONE"
)

// a Job struct contains information about a requested MapReduce job
type Job struct {
	JobID          int                // the client's MapReduce Job ID
	State          JobState           // the state of the client's Job
	Completed      []int              // which tasks are completed
	Ongoing        []int              // which tasks are ongoing
	DatabaseName   string             // the database that houses the data
	CollectionName string             // the collection where the data resides
	Tag            string             // the tag that identifies the relevant data
	FunctionID     primitive.ObjectID // the ID of the file stored in GridFS for mapf and reducef
	mu             sync.Mutex
}

// connections to MongoDB
type AviaryContext struct {
	intermediates *mongo.Collection
	functions     *mongo.Collection
}

// ClientContext struct
type ClientContext struct {
}

// TODO: "implement" coordinator fault-tolerance by transactionally storing Coordinator state in MongoDB
type AviaryCoordinator struct {
	jobs   map[int][]Job // map client id's to their list of Jobs
	counts map[int]int   // map client id's to their number of jobs (monotonically increasing JobID per-client)

	context AviaryContext // coordinator's connection with the database

	clerkCh  chan ClerkRequest
	workerCh chan WorkerRequest
	insertCh chan bson.D
	findCh   chan bson.D

	activeConnections map[UUID]int // slice of active connections to workers

	// oids for intermediate files in GridFS
	Files [][]primitive.ObjectID

	// placeholder
	count int
	mu    sync.Mutex
}
