package aviary

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"github.com/google/uuid"
)

const uri = "mongodb://127.0.0.1:27117,127.0.0.1:27118"

var gmutex sync.Mutex

func Gensym() uuid.UUID {
	gmutex.Lock()
	defer gmutex.Unlock()
	return uuid.New()
}

type Job struct {
	JobID     uuid.UUID
	Completed []uuid.UUID
	Ongoing   []uuid.UUID
	mu        sync.Mutex
}

// Coordinator maintains the map of a uuid.UUID (which corresponds to a client's MR request)
// to the associated jobs
type Coordinator struct {
	// UUID -> []Job
	jobs map[uuid.UUID]([]Job)
	mu   sync.Mutex
}

func (c *Coordinator) server() {
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

func MakeCoordinator() *Coordinator {
	c := Coordinator{}
	c.jobs[Gensym()] = make([]Job, 0)
	c.server()
	return &c
}

func (c *Coordinator) GetJobs() map[uuid.UUID][]Job {
	c.mu.Lock()
	defer c.mu.Unlock()

	_jobs := make(map[uuid.UUID]([]Job))
	for k, v := range c.jobs {
		_jobs[k] = v
	}
	return _jobs
}

func (c *Coordinator) RequestHandler(request *Request, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println(*request)
	fmt.Println(*reply)
	return nil
}
