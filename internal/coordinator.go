package aviary

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// start a thread that listens for RPCs from worker.go
func (c *AviaryCoordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	fmt.Printf("[Server] coordinator listening on port 1234\n")
	go http.Serve(l, nil)
}

// RPC handler for the Aviary Coordinator in distributing out tasks to Aviary Workers
// Each HTTP request has its own goroutine, i.e. GO runs the handler for each RPC in its own thread, so the fact
// that one handler is waiting needn't prevent the coordinator from processing other RPCs

func (c *AviaryCoordinator) RegisterWorker(request *RegisterWorkerRequest, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("[RegisterWorker] registering worker %d with port %d\n", request.WorkerID, request.WorkerPort)
	c.activeConnections[request.WorkerID] = request.WorkerPort
	reply.Status = OK
	return nil
}

func (c *AviaryCoordinator) broadcastMapTasks(request *ClerkRequest, jobId int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("[broadcastMapTasks] broadcasting tasks to %v connections\n\n", len(c.activeConnections))
	pk := 0
	for _, port := range c.activeConnections {
		// notify workers about a new job through RPC calls
		newRequest := CoordinatorRequest{
			Phase:          MAP,
			DatabaseName:   request.DatabaseName,
			CollectionName: request.CollectionName,
			Tag:            request.Tag,
			FunctionID:     request.FunctionID,
			Partition:      pk,
			JobID:          jobId,
		}
		go c.notifyWorker(port, &newRequest)
		pk++
	}
}

func (c *AviaryCoordinator) broadcastReduceTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("[Coordinator] Broadcasting Reduce tasks\n\n")
	pk := 0
	for _, port := range c.activeConnections {
		// notify workers about request jobs
		newRequest := CoordinatorRequest{
			Phase:          REDUCE,
			DatabaseName:   "db",
			CollectionName: "coll",
			Tag:            "wc",
			Partition:      pk,
			OIDs:           c.Files[pk], // TODO: this sometimes goes out of range when the workers rejoin (index 3 of size 3)
		}
		c.notifyWorker(port, &newRequest)
		pk++
	}
	c.count = 0
}

// NEW RPC HANDLERS to for workers to notify the coordinator that a Map/Reduce job is complete
func (c *AviaryCoordinator) MapComplete(request *MapCompleteRequest, reply *MapCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("[MapComplete] request.OIDs: %v\n\n", request.OIDs)

	c.Files[0] = append(c.Files[0], request.OIDs[0])
	c.Files[1] = append(c.Files[1], request.OIDs[1])
	c.Files[2] = append(c.Files[2], request.OIDs[2])

	c.count++
	if c.count == 3 {
		go c.broadcastReduceTasks()
	}

	return nil
}

func (c *AviaryCoordinator) ReduceComplete(request *ReduceCompleteRequest, reply *ReduceCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.count++
	if c.count == 3 {
		fmt.Printf("[Coordinator] All Reduce tasks complete!!\n")
		c.count = 0
		// TODO: add the ClientID and the JobID that the RPC is associated with
		// c.jobs[request.ClientID][request.JobID].State = DONE
	}

	return nil
}

// the Aviary Coordinator struct

// creates an Aviary Coordinator
func MakeCoordinator() *AviaryCoordinator {
	c := AviaryCoordinator{
		jobs:              make(map[int][]Job),
		counts:            make(map[int]int),
		context:           AviaryContext{},
		clerkRequestCh:    make(chan ClerkRequest),
		insertCh:          make(chan bson.D),
		findCh:            make(chan bson.D),
		activeConnections: make(map[UUID]int),
		Files:             make([][]primitive.ObjectID, 3),
		count:             0,
	}

	// establish connection to mongodb first before listening for RPCs
	ch := make(chan bool)
	go c.mongoConnection(ch)
	<-ch

	c.server()
	return &c
}

// main/aviarycoordinator.go calls this function
func StartCoordinator() {
	fmt.Println("[StartCoordinator] initializing")
	c := MakeCoordinator()
	c.listenForClerkRequests()
}

func (c *AviaryCoordinator) listenForClerkRequests() {
	for request := range c.clerkRequestCh { // c.startNewJob(&request)
		fmt.Printf("[listenForClerkRequests] received clerk request")
		c.mu.Lock()
		clientId := request.ClientID
		// TODO: Generate a random ID for the new job with something like:
		// jobID := IHash((c._gensym()).String())
		jobId := c.counts[clientId]
		c.counts[clientId]++
		c.jobs[clientId] = append(c.jobs[clientId], Job{
			JobID:          jobId,
			State:          PENDING,
			Completed:      make([]int, 0),
			Ongoing:        make([]int, 1),
			DatabaseName:   request.DatabaseName,
			CollectionName: request.CollectionName,
			Tag:            request.Tag,
			FunctionID:     request.FunctionID,
		})
		c.mu.Unlock()

		c.broadcastMapTasks(&request, jobId)
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
		// TODO: change so aviary still continues
		panic(err)
	}
	fmt.Println("[mongoConnection] coordinator connected to MongoDB!")

	// for now, we'll assume that all the users use one database, called "db"
	intermediates := client.Database("db").Collection("aviaryIntermediates")
	functions := client.Database("db").Collection("aviaryFuncs")

	c.context.intermediates = intermediates
	c.context.functions = functions

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
	jobs := make([]Job, len(c.jobs[clientID]))
	copy(jobs, c.jobs[clientID])
	return jobs
}

/* back-end functions */

// when client wants to start a new Job
func (c *AviaryCoordinator) startNewJob(request *ClerkRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := request.ClientID
	// generate a random id for the new job
	jobID := c.counts[clientID] // vs. jobID := IHash((c._gensym()).String())
	c.counts[clientID]++

	// insert a new job into the client's list of jobs
	c.jobs[clientID] = append(c.jobs[clientID], Job{
		JobID:          jobID,
		State:          PENDING,
		Completed:      make([]int, 0),
		Ongoing:        make([]int, 1),
		DatabaseName:   request.DatabaseName,
		CollectionName: request.CollectionName,
		Tag:            request.Tag,
		FunctionID:     request.FunctionID,
	})

	// instead of notifying workers, just have workers poll coordinator instead?
	i := 0
	for _, port := range c.activeConnections {
		// notify workers about a new job through RPC calls
		newRequest := CoordinatorRequest{
			Phase:          MAP,
			DatabaseName:   request.DatabaseName,
			CollectionName: request.CollectionName,
			Tag:            request.Tag,
			FunctionID:     request.FunctionID,
			Partition:      i,
		}
		go c.notifyWorker(port, &newRequest)
		i++
	}
}

// RPC request handler for incoming Clerk messages
func (c *AviaryCoordinator) ClerkRequestHandler(request *ClerkRequest, reply *ClerkReply) error {
	switch request.Type {
	case JOB:
		c.clerkRequestCh <- *request
		reply.Message = OK

	// show in progress jobs
	case SHOW:
		reply.Jobs = c.ShowJobs(request.ClientID)
		reply.Message = OK

	default:
	}

	return nil
}

// notify the worker at the given port about the new job
func (ac *AviaryCoordinator) notifyWorker(port int, request *CoordinatorRequest) {
	fmt.Printf("[notifyWorker] sending worker request: %v\n", request)
	fmt.Printf("[notifyWorker] request.FunctionID: %v\n", request.FunctionID)

	reply := CoordinatorReply{}

	ok := callRPC("AviaryWorker.CoordinatorRequestHandler", request, &reply, "", port)
	if !ok {
		log.Fatal("[notifyWorker] unable to notify worker\n") // TODO: handle this better that failing completely
	}
}

/* struct defs and functions for Coordinator to send RPC to Workers */

type CoordinatorRequest struct {
	Phase          Phase
	DatabaseName   string
	CollectionName string
	Tag            string
	FunctionID     primitive.ObjectID
	Partition      int
	JobID          int

	// intermediate files in gridfs
	OIDs []primitive.ObjectID
}

// TODO: maybe write a String() method for CoordinatorRequest

type CoordinatorReply struct {
	Reply   string
	Message string
}
