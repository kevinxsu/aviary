package aviary

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoDB connection string
const uri = "mongodb://126.0.0.1:27117,127.0.0.1:27118"

type Phase = string

const (
	INIT        = "INIT"
	MAP         = "MAP"
	MAP_DONE    = "MAP_DONE"
	REDUCE      = "REDUCE"
	REDUCE_DONE = "REDUCE_DONE"
	IDLE        = "IDLE"
)

type Request struct {
	WorkerRequest string
}

type Reply struct {
	CoordinatorReply string
}

func coordinatorSock() string {
	s := "/var/tmp/aviary-coordinator-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock() string {
	s := "/var/tmp/aviary-worker-"
	s += strconv.Itoa(os.Getuid())
	return s
}

/* struct defs and functions for Coordinator to send RPC to Workers */

type CoordinatorRequest struct {
	Phase          Phase
	DatabaseName   string
	CollectionName string
	Tag            string
	FunctionID     primitive.ObjectID
	Partition      int

	// intermediate files in gridfs
	OIDs []primitive.ObjectID
}

type CoordinatorReply struct {
	Reply   string
	Message string
}

// TODO: add slice of active connections to coordinator and concurrently notify all of them
func (ac *AviaryCoordinator) notifyWorkers(rpcname string, args interface{}, reply interface{}) bool {
	// TODO: iterate over all the ports in the coordinator slices and concurrently notify workers

	fmt.Println("(coord) Entered notifyWorkers()")
	fmt.Println(ac.activeConnections)

	for _, port := range ac.activeConnections {
		c, err := rpc.DialHTTP("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			log.Fatal("dialing: ", err)
		}
		defer c.Close()
		err = c.Call(rpcname, args, reply)
		if err != nil {
			return false
		}
	}
	return true
}

// TODO:
// coordinator notifies workers about a new job (calls notifyWorkers())
func (c *AviaryCoordinator) CoordinatorCall(request *CoordinatorRequest, reply *CoordinatorReply) error {
	ok := c.notifyWorkers("AviaryWorker.CoordinatorRequestHandler", request, reply)
	if !ok {
		fmt.Println("something went wrong with trying to send rpc from coord to worker")
	}
	// c.notifyWorkers("AviaryWorker.CoordinatorRequestHandler", request, reply)
	return nil
}

type ClerkType = string

const (
	JOB  ClerkType = "JOB"
	SHOW           = "SHOW"
)

type ReplyType = string

const (
	OK ReplyType = "OK"
)

/* struct defs and functions for Coordinator to receive RPCs from Clerk */

// RPC structs and handlers from Clerks
type ClerkRequest struct {
	Type           ClerkType
	ClientID       int
	DatabaseName   string
	CollectionName string
	Tag            string
	FunctionID     primitive.ObjectID
}

type ClerkReply struct {
	Message ReplyType // coordinator response message
	Jobs    []Job     // in-progress jobs
}

func (c *AviaryCoordinator) ClerkRequestHandler(request *ClerkRequest, reply *ClerkReply) error {
	switch request.Type {
	case JOB:
		c.clerkCh <- *request
		reply.Message = OK

	// show in progress jobs
	case SHOW:
		reply.Jobs = c.ShowJobs(request.ClientID)
		reply.Message = OK

	default:
	}

	return nil
}

/* struct defs and functions for Coordinator to receive RPCs from Worker */

type WorkerState struct {
	JobID    uuid.UUID
	WorkerID uuid.UUID
	Action   Phase
	Key      interface{}
	Value    interface{}
}

type WorkerRequest struct {
	WorkerID    UUID
	WorkerState Phase
	WorkerPort  int // if INIT Phase, should only be WorkerID, WorkerState, and WorkerPort

	// the id of the intermediate data from Map phase (stored in GridFS)
	// ObjectID primitive.ObjectID
	OIDs []primitive.ObjectID
}

type WorkerReply struct {
	Reply ReplyType
}

// RPC handler for the Aviary Coordinator in distributing out tasks to Aviary Workers
// Each HTTP request has its own goroutine, i.e. GO runs the handler for each RPC in its own thread, so the fact
// that one handler is waiting needn't prevent the coordinator from processing other RPCs
func (c *AviaryCoordinator) WorkerRequestHandler(request *WorkerRequest, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch request.WorkerState {
	case INIT:
		fmt.Printf("(coord) WorkerRequestHandler: case INIT. Going to add WorkerPort %d to map.\n", request.WorkerPort)
		c.activeConnections[request.WorkerID] = request.WorkerPort
		reply.Reply = OK

		fmt.Println(c.activeConnections)

	case MAP_DONE:
		fmt.Printf("(coord) WorkerRequestHandler: case MAP_DONE.")
		// incr count
		c.count++

		// append to c.Files
		// c.Files = append(c.Files, request.OIDs...)

		c.Files[0] = append(c.Files[0], request.OIDs...)
		c.Files[1] = append(c.Files[1], request.OIDs...)
		c.Files[2] = append(c.Files[2], request.OIDs...)

		// start reducing
		if c.count == 3 {
			// TODO:
			// go func(c *AviaryCoordinator) {
			fmt.Println("entered anonymous function")
			i := 0
			for _, port := range c.activeConnections {
				// oids := make([]primitive.ObjectID, 0)
				// TODO: use regex
				// for j := 0; j < len(c.Files); j++ {
				// 	// 10
				// 	if c.FileNames[j][10] == i {
				// 		oids = append(oids, c.Files[j])
				// 	}
				// }

				// notify workers about request jobs
				newRequest := CoordinatorRequest{
					Phase:          REDUCE,
					DatabaseName:   "db",
					CollectionName: "coll",
					Tag:            "wc",
					Partition:      i,
					OIDs:           c.Files[i],
				}
				c.notifyWorker(port, &newRequest)
				i++
			}
			// }(c)
		}

		reply.Reply = OK

	case REDUCE_DONE:
		fmt.Printf("(coord) WorkerRequestHandler: case REDUCE_DONE.")

	default:
	}
	reply.Reply = OK
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *AviaryCoordinator) server() {
	fmt.Println("Entered AviaryCoordinator.server()")
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go http.Serve(l, nil)
}
