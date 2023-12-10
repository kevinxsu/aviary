package aviary

import (
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

// RPC structs for workers and the coordinator
type MapCompleteRequest struct {
	JobID    int
	ClientID int
	WorkerID UUID
	OIDs     []primitive.ObjectID
}

type MapCompleteReply struct {
	Status ReplyType
}

type ReduceCompleteRequest struct {
	JobID    int // the ID of the client's MapReduce job
	ClientID int // the client's ID is also just an int
}
type ReduceCompleteReply struct {
	JobID    int // the ID of the client's MapReduce job
	ClientID int // the client's ID is also just an int
}

type RegisterWorkerRequest struct {
	WorkerID   UUID
	WorkerPort int
}

type RegisterWorkerReply struct {
	Status ReplyType
}
