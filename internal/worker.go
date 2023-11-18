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

type AviaryWorker struct {
	WorkerID   UUID       // the worker's identifier
	InProgress []Job      // inprogress jobs (want to allow for concurrent jobs)
	Completed  []Job      // completed jobs
	mu         sync.Mutex // for concurrent requests

	requestCh chan CoordinatorRequest // channel for incoming jobs sent by the coordinator
	findCh    chan bson.D             // channel for worker to find stuff? (NOT SURE IF THIS EVEN WORKS OR IS THE BEST WAY)
}

// main/aviaryworker.go calls this function
func StartWorker() {
	w := MakeWorker()
	w.Start()
}

// creates an Aviary Worker
func MakeWorker() *AviaryWorker {
	w := AviaryWorker{
		WorkerID:   Gensym(),
		InProgress: make([]Job, 0),
		Completed:  make([]Job, 0),
		requestCh:  make(chan CoordinatorRequest),
		findCh:     make(chan bson.D),
	}

	// establish connection with mongodb first
	ch := make(chan bool)
	go w.mongoConnection(ch)
	<-ch

	// start listening for RPCs
	w.server()
	return &w
}

// long-running goroutine for worker to connect to it's local shard
func (w *AviaryWorker) mongoConnection(ch chan bool) {
	// TODO: figure out how to make the worker discover the mongod process
	// but atm not running workers inside the container, just have it query the routers for now
	// uri := "mongodb://126.0.0.1:27122" // <- seems like we can't directly query the mongod shards just through the uri string
	// but we can interact with the shard's data through docker exec -it [shard-x-y] mongosh

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Aviary Worker connected to MongoDB")

	collection := client.Database("MyDatabase").Collection("MyCollection")
	ch <- true
	for {
		select {
		case filter := <-w.findCh:
			fmt.Println("Case: filter := <-w.findCh")
			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var res interface{}
			err := collection.FindOne(ctxt, filter).Decode(&res)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					fmt.Println("WARNING: Aviary Worker couldn't find documents")
					continue
				}
				log.Fatal("mongo find error: ", err)
			}
			fmt.Printf("Worker found %v\n", res)
		}
	}
}

// call in goroutine?
// does the map function
func (w *AviaryWorker) Map(request CoordinatorRequest) {
	mapFunc := request.MapFunc
	reduceFunc := request.ReduceFunc
	dbName := request.DatabaseName
	collName := request.CollectionName
	fmt.Println(mapFunc)
	fmt.Println(reduceFunc)
	fmt.Println(dbName)
	fmt.Println(collName)

	// TODO: write a better struct to encapsulate db.Find() queries
	fmt.Println("about to send bson.D{{}} through findCh")
	w.findCh <- bson.D{{"asdfasdfasdf", bson.M{"$exists": "true"}}}
	fmt.Println("after w.findCh <- ..")
}

// call in goroutine
func (w *AviaryWorker) Reduce() {
	// something
}

// wait to be assigned a new job from the coordinator
func (w *AviaryWorker) Start() {
	for {
		// await new job
		job := <-w.requestCh
		fmt.Printf("worker got new job: %v\n", job)

		switch job.Phase {
		case MAP:
			fmt.Println("job was a map")

			go w.Map(job)

		case REDUCE:
			fmt.Println("job was reduce")
		default:
		}

	}
}

// how is this going to work?
// the worker is it's own process on the container, alongside the mongod process
// the worker gets pinged, it tries to find the data from the aviary-funcs collection
// then it creates a new file and fork-execs?
// or maybe do it the way 65840 did it
//
//	client has the go file
//	client compiles the go file into a .so object, and then convert that into a byte slice
//	client sends path/to/file when querying the Clerk to start a new MR job
// 	Clerk reads in the .so file and converts it into a byte slice, save it into an RPC call to Coordinator
//	Coordinator inserts the function byte slices before notifying Workers
// 	Workers can pull the function binary from db.aviary-funcs
//  Workers can store intermediate results in db.aviary-intermediates

/*
	but how to efficiently represent the function?
	{
		client_id: UUID,
		job_id: UUID,
		mapFunc:	"...", // name of the function
		reduceFunc: "...", // name of the function
	}
*/

/*
func (w *AviaryWorker) findFunction(clientID UUID, jobID UUID, dbName string, phase Phase) {
	switch phase {
	case MAP:

	case REDUCE:
	}
}
func (w *AviaryWorker) findMapFunction(clientID UUID, jobID UUID, mapFunc string) { }
func (w *AviaryWorker) findReduceFunction(clientID UUID, jobID UUID, mapFunc string) { }
*/

/* example code to send something to the coordinator
id := Gensym()
s := id.String()
request := Request{WorkerRequest: s}
reply := Reply{}
WorkerCall(&request, &reply)
fmt.Println(reply.CoordinatorReply)

if reply.CoordinatorReply != "" {
	fmt.Println("Coordinator Reply was empty, exiting...")
	os.Exit(0)
}
*/
