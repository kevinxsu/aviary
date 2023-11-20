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
	INIT   = "INIT"
	MAP    = "MAP"
	REDUCE = "REDUCE"
	IDLE   = "IDLE"
)

type WorkerState struct {
	JobID    uuid.UUID
	WorkerID uuid.UUID
	Action   Phase
	Key      interface{}
	Value    interface{}
}

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
}

type CoordinatorReply struct {
	Message string
}

// func _notifyWorkers(rpcname string, args interface{}, reply interface{}) bool {
// 	for _, port := range
// 	c, err :=
// }

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

	// c, err := rpc.DialHTTP("tcp", ":1235")
	// if err != nil {
	// 	// TODO: if the worker doesn't exist, the coordinator shouldnt crash
	// 	log.Fatal("dialing: ", err)
	// }
	// defer c.Close()
	// err = c.Call(rpcname, args, reply)

	// return err == nil
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

type WorkerRequest struct {
	WorkerID    UUID
	WorkerState Phase
	WorkerPort  int // if INIT Phase, should only be WorkerID, WorkerState, and WorkerPort
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
