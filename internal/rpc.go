package aviary

import (
	"os"
	"strconv"

	"github.com/google/uuid"
)

type ActionType string

const (
	INIT   = "INIT"
	MAP    = "MAP"
	REDUCE = "REDUCE"
	IDLE   = "IDLE"
)

type WorkerState struct {
	JobID    uuid.UUID
	WorkerID uuid.UUID
	Action   ActionType
	Key      interface{}
	Value    interface{}
}

type Request struct {
	Req string
}

type Reply struct {
	Rep string
}

func coordinatorSock() string {
	s := "/var/tmp/aviary-"
	s += strconv.Itoa(os.Getuid())
	return s
}
