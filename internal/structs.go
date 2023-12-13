package aviary

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	ClientID       int                // the client's ID
	JobID          int                // the client's MapReduce Job ID
	State          JobState           // the state of the client's Job
	Completed      []int              // which tasks are completed
	Ongoing        []int              // which tasks are ongoing
	DatabaseName   string             // the database that houses the data
	CollectionName string             // the collection where the data resides
	Tag            string             // the tag that identifies the relevant data
	FunctionID     primitive.ObjectID // the ID of the file stored in GridFS for mapf and reducef
	FileOIDs       []primitive.ObjectID
	mu             sync.Mutex
}

// TODO: "implement" coordinator fault-tolerance by transactionally storing Coordinator state in MongoDB
type AviaryCoordinator struct {
	jobs              map[int][]Job // map client id's to their list of Jobs
	counts            map[int]int   // map client id's to their number of jobs (monotonically increasing JobID per-client)
	clerkRequestCh    chan ClerkRequest
	insertCh          chan bson.D
	findCh            chan bson.D
	activeConnections map[UUID]int           // slice of active connections to workers
	Files             [][]primitive.ObjectID // oids for intermediate files in GridFS
	mu                sync.Mutex
	dropCollectionsCh chan bool
	// placeholder, TODO: fix this so we have dynamic number of workers
	count     int
	startTime time.Time
}

// wrap, to pass through channel
type ReducedResults struct {
	keyvalues []KeyValue // should be sorted
	filename  string
	jobID     int
	clientID  int
	partition int
}
