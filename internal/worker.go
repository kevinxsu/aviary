package aviary

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AviaryWorker struct {
	WorkerID   UUID       // the worker's identifier
	InProgress []Job      // inprogress jobs (want to allow for concurrent jobs)
	Completed  []Job      // completed jobs
	mu         sync.Mutex // for concurrent requests

	requestCh  chan CoordinatorRequest // channel for incoming jobs sent by the coordinator
	findCh     chan bson.D             // channel for worker to find stuff? (NOT SURE IF THIS EVEN WORKS OR IS THE BEST WAY)
	downloadCh chan primitive.ObjectID // download
	startCh    chan bool

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	port int // the worker's port

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
		downloadCh: make(chan primitive.ObjectID),
		startCh:    make(chan bool),

		mapf:    nil,
		reducef: nil,

		port: -1,
	}

	// establish connection with mongodb first
	ch := make(chan bool)
	go w.mongoConnection(ch)
	<-ch

	// start listening for RPCs
	w.server()

	// try to send RPC to coordinator to notify it of our port
	w.subscribe()

	return &w
}

// continuously try to send RPCs to the coordinator to subscribe to messages?
func (w *AviaryWorker) subscribe() {
	request := WorkerRequest{
		WorkerID:    w.WorkerID,
		WorkerState: INIT,
		WorkerPort:  w.port,
	}
	reply := WorkerReply{}
	// keep trying to send until coordinator receives RPC
	for {
		ok := workerCall("AviaryCoordinator.WorkerRequestHandler", &request, &reply)
		if ok {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("Worker %v subscribed to Coordinator!\n", w.WorkerID)
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
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
	fmt.Println("Aviary Worker connected to MongoDB!")

	db := client.Database("db")
	collection := db.Collection("coll")
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

		case fileID := <-w.downloadCh:
			fmt.Println("(Aviary Worker) case: downloadCh")

			grid_opts := options.GridFSBucket().SetName("aviaryFuncs")
			bucket, err := gridfs.NewBucket(db, grid_opts)
			if err != nil {
				panic(err)
			}

			downloadStream, err := bucket.OpenDownloadStream(fileID)
			if err != nil {
				panic(err)
			}

			tmpFilename := "tmp" + w.WorkerID.String() + "lib.so"

			file, err := os.Create(tmpFilename)
			if err != nil {
				panic(err)
			}

			_, err = io.Copy(file, downloadStream)
			if err != nil {
				panic(err)
			}

			// file downloaded now. need to update worker's map and reduce functiosn
			mapf, reducef := loadPlugin(tmpFilename)
			w.mapf = mapf
			w.reducef = reducef
			w.startCh <- true

			defer downloadStream.Close()
			defer file.Close()
		}
	}
}

// wait to be assigned a new job from the coordinator
func (w *AviaryWorker) Start() {
	for {
		// await new job
		job := <-w.requestCh
		fmt.Printf("Worker got new job: %v\n", job)

		// download the map and reduce functions
		w.downloadCh <- job.FunctionID

		// block until the channel
		<-w.startCh

		switch job.Phase {
		case MAP:
			fmt.Println("(Aviary Worker) case: MAP")

			/*
				type Job struct {
					JobID int
					State JobState
					Completed []int
					Ongoing []int
					mu sync.Mutex

					DatabaseName string
					CollectionName string
					Tag string
					FunctionId primitive.ObjectID
				}
			*/

			// just create a new connection for now
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
			fmt.Println("Aviary Worker connected to MongoDB!")

			db := client.Database(job.DatabaseName)
			collection := db.Collection(job.CollectionName)

			// filter by partition number
			filter := bson.D{{"partition", job.Partition}}

			// find the stuff
			cursor, err := collection.Find(context.TODO(), filter)
			if err != nil {
				panic(err)
			}

			var results []InputData
			if err = cursor.All(context.TODO(), &results); err != nil {
				panic(err)
			}

			for _, result := range results {
				res, _ := json.Marshal(result)
				fmt.Println(string(res))
			}

			intermediates := make([]KeyValue, 0)

			for _, result := range results {
				res, _ := json.Marshal(result)
				contents := string(res)

				// intermediate has type []KeyValue
				intermediate := w.mapf("", contents)
				intermediates = append(intermediates, intermediate...)
			}

			// fmt.Println(len(intermediates))
			// fmt.Println(intermediates[0])
			// fmt.Println(intermediates)

		case REDUCE:
			fmt.Println("(Aviary Worker) case: REDUCE")

		default:
		}
	}
}

// TODO: for workers on startup, workers need to send an RPC to the coordinator
// and provide the coordinator with it's HTTP endpoint
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
	fmt.Printf("ASDASDASD error in workerCall: %v\n", err)
	return false
}

// calls the Coordinator's WorkerRequestHandler
func WorkerCall(request *WorkerRequest, reply *WorkerReply) {
	for {
		ok := workerCall("AviaryCoordinator.WorkerRequestHandler", request, reply)
		if ok {
			return
		}
		time.Sleep(time.Second)
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

	var l net.Listener
	var err error

	for i := 1235; ; i++ {
		l, err = net.Listen("tcp", ":"+strconv.Itoa(i))
		if err == nil {
			w.port = i
			break
		}
	}
	fmt.Printf("(worker) server(): found good port: %d\n", w.port)
	go http.Serve(l, nil)
}

// Worker's RPC handler for Coordinator notifications
func (w *AviaryWorker) CoordinatorRequestHandler(request *CoordinatorRequest, reply *CoordinatorReply) error {
	fmt.Println("(worker) Entered CoordinatorRequestHandler")
	fmt.Println(*request)

	w.requestCh <- *request
	fmt.Println("after w.requestCh <- *request ")
	reply.Message = OK
	return nil
}
