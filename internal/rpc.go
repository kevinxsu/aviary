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

func notifyWorkers(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", ":1235")
	if err != nil {
		// TODO: if the worker doesn't exist, the coordinator shouldnt crash
		log.Fatal("dialing: ", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)

	return err == nil
}

// coordinator notifies workers about a new job
func (c *AviaryCoordinator) CoordinatorCall(request *CoordinatorRequest, reply *CoordinatorReply) error {
	ok := notifyWorkers("AviaryWorker.CoordinatorRequestHandler", request, reply)
	if !ok {
		fmt.Println("something went wrong with trying to send rpc from coord to worker")
	}
	return nil
}

type ClerkType = string

const (
	JOB  ClerkType = "JOB"
	SHOW           = "SHOW"
	// INSERT           = "INSERT"
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
}

type WorkerReply struct {
}

// RPC handler for the Aviary Coordinator in distributing out tasks to Aviary Workers
// Each HTTP request has its own goroutine, i.e. GO runs the handler for each RPC in its own thread, so the fact
// that one handler is waiting needn't prevent the coordinator from processing other RPCs
func (c *AviaryCoordinator) WorkerRequestHandler(request *Request, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println(request.WorkerRequest)
	fmt.Println(reply.CoordinatorReply)
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

func workerCall(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func WorkerCall(request *Request, reply *Reply) {
	ok := workerCall("AviaryCoordinator.WorkerRequestHandler", request, reply)
	if !ok {
		os.Exit(0)
	}
}

// potential issue: how does coordinator broadcast to all workers?
// dont think we can reuse the same port
// but similar to coord, start a thread that listens for RPCs from coordinator
func (w *AviaryWorker) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	// sockname := coordinatorSock()
	sockname := workerSock()
	os.Remove(sockname)
	l, err := net.Listen("tcp", ":1235")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go http.Serve(l, nil)
}

func (w *AviaryWorker) CoordinatorRequestHandler(request *CoordinatorRequest, reply *CoordinatorReply) error {
	fmt.Println("worker entered coordinator request handler")
	fmt.Println(*request)

	w.requestCh <- *request
	fmt.Println("finished sending request over requestCh")
	reply.Message = "OK"
	return nil
}
