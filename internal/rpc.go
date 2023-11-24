package aviary

import (
	"os"
	"strconv"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoDB connection string
const uri = "mongodb://126.0.0.1:27117,127.0.0.1:27118"
const URI = "mongodb://126.0.0.1:27117,127.0.0.1:27118"

type Phase = string

const (
	INIT        Phase = "INIT"
	MAP               = "MAP"
	REDUCE            = "REDUCE"
	IDLE              = "IDLE"
	MAP_DONE          = "MAP_DONE"
	REDUCE_DONE       = "REDUCE_DONE"
)

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
