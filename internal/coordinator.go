package aviary

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JobState = int

const (
	MAPPING JobState = iota
	REDUCING
	DONE
)

// a Job struct contains information about the requested MapReduce job
// TODO: figure out how to actually do the MapReduce... given this information
type Job struct {
	// actual job stuff
	JobID     int
	State     JobState
	Completed []int
	Ongoing   []int
	mu        sync.Mutex

	// extraneous details about the job from a Clerk RPC
	MapFunc        string
	ReduceFunc     string
	DatabaseName   string
	CollectionName string
}

// TODO: "implement" coordinator fault-tolerance
// by transactionally storing Coordinator state in MongoDB
type AviaryCoordinator struct {
	// int to identify clients, and maps to their jobs
	jobs     map[int][]Job
	mu       sync.Mutex
	clerkCh  chan ClerkRequest
	workerCh chan WorkerRequest
	insertCh chan bson.D
	findCh   chan bson.D

	insertFunctionCh chan MongoFunction
}

// creates an Aviary Coordinator
func MakeCoordinator() *AviaryCoordinator {
	c := AviaryCoordinator{}
	c.jobs = make(map[int][]Job)
	c.clerkCh = make(chan ClerkRequest)
	c.workerCh = make(chan WorkerRequest)

	c.insertCh = make(chan bson.D)
	c.findCh = make(chan bson.D)

	c.insertFunctionCh = make(chan MongoFunction)

	// establish connection to mongodb first before listening for RPCs
	ch := make(chan bool)
	go c.mongoConnection(ch)
	<-ch

	c.server()
	return &c
}

// main/aviarycoordinator.go calls this function
func StartCoordinator() {
	fmt.Println("Entered StartCoordinator")
	c := MakeCoordinator()
	c.Start()
}

// continuously poll for new requests
func (c *AviaryCoordinator) Start() {
	for {
		select {
		// a client request
		case request := <-c.clerkCh:
			// NOTE: client can send clerk different kinds of requests, but any non-MR job
			// request will be handled in the RPC handler rather than here
			// BUT not sure if this is good design, probably better to just have it all in one place?
			fmt.Printf("clerkCh with %v\n", request)
			c.startNewJob(&request)

		// a worker notification
		case request := <-c.workerCh:
			fmt.Printf("workerCh with %v\n", request)
			// TODO: give Worker W more tasks after coordinator receives a notification from W
		}
	}
}

// long-running goroutine to maintain a connection between Coordinator and MongoDB
func (c *AviaryCoordinator) mongoConnection(ch chan bool) {
	// set to stable version 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	// create a new client and connect it to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// send ping to confirm successful connection
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Aviary Coordinator connected to MongoDB")

	intermediates := client.Database("MyDatabase").Collection("aviary-intermediates")
	functions := client.Database("MyDatabase").Collection("aviary-funcs")

	// unblock MakeCoordinator thread
	ch <- true

	// continuously poll for events
	for {
		select {
		case toInsert := <-c.insertCh:
			fmt.Println("Case: toInsert := <-c.insertCh")

			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := functions.InsertOne(ctxt, toInsert)
			if err != nil {
				log.Fatal("mongo insert error: ", err)
			}
			fmt.Printf("Coordinator inserted %v with id\n", res, res.InsertedID)

		case function := <-c.insertFunctionCh:
			fmt.Println("Case: function := <-c.insertFunctionCh")

			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := functions.InsertOne(ctxt, function)
			if err != nil {
				log.Fatal("mongo insert function error: ", err)
			}
			fmt.Printf("Coordinator inserted %v\n", res)

		case filter := <-c.findCh:
			fmt.Println("Case: filter := <-c.findCh")
			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := intermediates.Find(ctxt, filter)
			if err != nil {
				log.Fatal("mongo find error: ", err)
			}
			fmt.Printf("Coordinator found %v\n", res)
		}
	}
}

/* front-end functions */

// response to when client wants to see their job
func (c *AviaryCoordinator) ShowJobs(clientID int) []Job {
	c.mu.Lock()
	defer c.mu.Unlock()
	// c._prettyPrintJobs()

	jobs := make([]Job, len(c.jobs[clientID]))
	copy(jobs, c.jobs[clientID])
	return jobs
}

/* back-end functions */

// struct definition for a mongo function insertion
type MongoFunction struct {
	ClientID   int
	JobID      int
	MapFunc    string
	ReduceFunc string
}

// when client wants to start a new Job
func (c *AviaryCoordinator) startNewJob(request *ClerkRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := request.ClientID
	mapFunc := request.MapFunc
	reduceFunc := request.ReduceFunc
	dbName := request.DatabaseName
	collName := request.CollectionName

	// generate a random job id
	jobID := IHash((c._gensym()).String())

	// create a new Job struct
	newJob := Job{
		JobID:          jobID,
		Completed:      make([]int, 0),
		Ongoing:        make([]int, 1),
		MapFunc:        mapFunc,
		ReduceFunc:     reduceFunc,
		DatabaseName:   dbName,
		CollectionName: collName,
	}
	// insert the new job into the client's list of jobs
	c.jobs[clientID] = append(c.jobs[clientID], newJob)
	c._prettyPrintJobs()

	// insert the map/reduce functions into mongodb before notifying workers
	c.insertFunctionCh <- MongoFunction{
		ClientID:   clientID,
		JobID:      jobID,
		MapFunc:    mapFunc,
		ReduceFunc: reduceFunc,
	}

	fmt.Println("after c.insertFunctionCh <- ..")

	// notify workers about a new job through RPC calls
	newRequest := CoordinatorRequest{
		Phase:          MAP,
		MapFunc:        mapFunc,
		ReduceFunc:     reduceFunc,
		DatabaseName:   dbName,
		CollectionName: collName,
	}
	newReply := CoordinatorReply{}

	// maybe call as goroutine?
	err := c.CoordinatorCall(&newRequest, &newReply)

	if err != nil {
		log.Fatal(err)
	}

	if newReply.Message != "OK" {
		log.Fatal("CoordinatorCall RPC to Worker Reply was not OK")
	}
}
