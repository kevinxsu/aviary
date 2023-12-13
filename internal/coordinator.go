package aviary

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// start a thread that listens for RPCs from worker.go
func (c *AviaryCoordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	CPrintf("[Server] coordinator listening on port 1234\n")
	go http.Serve(l, nil)
}

// RPC handler for the Aviary Coordinator in distributing out tasks to Aviary Workers
// Each HTTP request has its own goroutine, i.e. GO runs the handler for each RPC in its own thread, so the fact
// that one handler is waiting needn't prevent the coordinator from processing other RPCs

func (c *AviaryCoordinator) RegisterWorker(request *RegisterWorkerRequest, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	CPrintf("[RegisterWorker] registering worker %d with port %d\n", request.WorkerID, request.WorkerPort)

	c.activeConnections[request.WorkerID] = request.WorkerPort
	reply.Status = OK
	return nil
}

func (c *AviaryCoordinator) broadcastMapTasks(request *ClerkRequest, jobID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	CPrintf("[broadcastMapTasks] broadcasting tasks to %v connections\n", len(c.activeConnections))

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
			JobID:          jobID,
			ClientID:       request.ClientID,
		}
		go c.notifyWorker(port, &newRequest)
		pk++
	}
}

// TODO: generalize hardcoded Tag field
func (c *AviaryCoordinator) broadcastReduceTasks(request *MapCompleteRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	CPrintf("[Coordinator] Broadcasting Reduce tasks\n")

	pk := 0
	for _, port := range c.activeConnections {
		// notify workers about request jobs
		newRequest := CoordinatorRequest{
			Phase:          REDUCE,
			DatabaseName:   "db",
			CollectionName: "coll",
			Tag:            "wc",
			Partition:      pk,
			JobID:          request.JobID,
			ClientID:       request.ClientID,
			OIDs:           c.Files[pk],
		}
		c.notifyWorker(port, &newRequest)
		pk++
	}
	c.count = 0
}

// NEW RPC HANDLERS to for workers to notify the coordinator that a Map/Reduce job is complete
func (c *AviaryCoordinator) MapComplete(request *MapCompleteRequest, reply *MapCompleteReply) error {
	// TODO: generalize to n workers (how to implement leave/join?)
	c.mu.Lock()
	defer c.mu.Unlock()
	// c.Files[0] = append(c.Files[0], request.OIDs[0])
	// c.Files[1] = append(c.Files[1], request.OIDs[1])
	// c.Files[2] = append(c.Files[2], request.OIDs[2])
	c.count++
	if c.count == 3 {
		go c.broadcastReduceTasks(request)
	}

	return nil
}

// TODO: generalize to N workers and change c.count
func (c *AviaryCoordinator) ReduceComplete(request *ReduceCompleteRequest, reply *ReduceCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.count++

	// TODO: Add additional information so the user knows where to get their reduced data
	if c.count == 3 {
		c.count = 0

		// update the state of the Job{JobID} associated ClientID
		// c.jobs[request.ClientID][request.JobID].mu.Lock()
		// defer c.jobs[request.ClientID][request.JobID].mu.Unlock()
		c.jobs[request.ClientID][request.JobID].State = DONE
		CPrintf("[Coordinator] REDUCE TASKS COMPLETED\n")

		// TODO: clear the collection (this means no concurrent jobs)
		c.dropCollectionsCh <- true
	}
	c.jobs[request.ClientID][request.JobID].FileOIDs = append(c.jobs[request.ClientID][request.JobID].FileOIDs, request.OID)

	return nil
}

// creates an Aviary Coordinator
func MakeCoordinator() *AviaryCoordinator {
	c := AviaryCoordinator{
		jobs:              make(map[int][]Job),
		counts:            make(map[int]int),
		clerkRequestCh:    make(chan ClerkRequest),
		insertCh:          make(chan bson.D),
		findCh:            make(chan bson.D),
		activeConnections: make(map[UUID]int),
		Files:             make([][]primitive.ObjectID, 3),
		dropCollectionsCh: make(chan bool),
		count:             0,
	}

	// establish connection to MongoDB before listening for RPCs
	ch := make(chan bool)
	go c.mongoConnection(ch)
	<-ch

	c.server()
	return &c
}

// main/aviarycoordinator.go calls this function
func StartCoordinator() {
	CPrintf("[StartCoordinator] initializing\n")
	c := MakeCoordinator()
	c.listenForClerkRequests()
}

func (c *AviaryCoordinator) listenForClerkRequests() {
	for request := range c.clerkRequestCh { // c.startNewJob(&request)
		CPrintf("[listenForClerkRequests] received clerk request\n")
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
			ClientID:       clientId,
			FileOIDs:       make([]primitive.ObjectID, 0),
		})
		c.mu.Unlock()
		c.broadcastMapTasks(&request, jobId)
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
	jobID := c.counts[clientID] // vs. jobID := IHash((c._gensym()).String()) // generate a random id for the new job
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
		ClientID:       clientID,
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
			ClientID:       request.ClientID,
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

	// download stuff
	case GET:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.jobs[request.ClientID][request.JobID].State != DONE {
			reply.Message = NOTREADY
			return nil
		}

		// otherwise, copy the oids
		reply.OIDS = make([]primitive.ObjectID, len(c.jobs[request.ClientID][request.JobID].FileOIDs))
		copy(reply.OIDS, c.jobs[request.ClientID][request.JobID].FileOIDs)
		reply.Message = OK

	default:
	}

	return nil
}

// notify the worker at the given port about the new job
func (ac *AviaryCoordinator) notifyWorker(port int, request *CoordinatorRequest) {
	reply := CoordinatorReply{}
	ok := callRPC("AviaryWorker.CoordinatorRequestHandler", &request, &reply, "", port)
	if !ok {
		// TODO: handle this better that failing completely
		log.Fatal("{coord}[notifyWorker] unable to notify worker\n")
	}
	CPrintf("[notifyWorker] Coordinator notified a Worker with request: %v\n", request)
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
	ClientID       int
	OIDs           []primitive.ObjectID // intermediate files in gridfs
}

// TODO: maybe write a String() method for CoordinatorRequest

type CoordinatorReply struct {
	Reply   string
	Message string
}
