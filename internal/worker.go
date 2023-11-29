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
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
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

	// channel to upload intermediate file
	intermediateCh chan string

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

			if runtime.GOOS != "darwin" {
				grid_opts := options.GridFSBucket().SetName("aviaryFuncs")
				bucket, err := gridfs.NewBucket(db, grid_opts)
				if err != nil {
					fmt.Printf("failed here1\n")
					panic(err)
				}

				fmt.Println("(worker) opening download stream")
				downloadStream, err := bucket.OpenDownloadStream(fileID)
				if err != nil {
					fmt.Printf("failed while trying to open the download stream\n")
					panic(err)
				}

				tmpFilename := "tmp" + w.WorkerID.String() + "lib.so"

				fmt.Printf("(worker) creating tmp file: %s\n", tmpFilename)
				file, err := os.Create(tmpFilename)
				if err != nil {
					fmt.Printf("failed to create the tmp file\n")
					panic(err)
				}

				fmt.Printf("(worker) copying downloadStream contents to tmp file\n")
				_, err = io.Copy(file, downloadStream)
				if err != nil {
					fmt.Printf("failed to copy the file\n")
					panic(err)
				}

				// file downloaded now. need to update worker's map and reduce functions
				fmt.Printf("(worker) loading Map/Reduce functions from plugin\n")
				mapf, reducef := loadPlugin(tmpFilename) // TODO BUG: killed here
				w.mapf = mapf
				w.reducef = reducef

				defer downloadStream.Close()
				defer file.Close()
			} else {
				fmt.Printf("Skipping download step for macOS")
				w.mapf = Map
				w.reducef = Reduce
			}

			w.startCh <- true
		}
	}
}

func (w *AviaryWorker) handleMap(job CoordinatorRequest) []primitive.ObjectID {
	fmt.Println("(worker) handling Map task")

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

	// for _, result := range results {
	// 	res, _ := json.Marshal(result)
	// 	fmt.Println(string(res))
	// }

	intermediates := make([]KeyValue, 0)

	for _, result := range results {
		res, _ := json.Marshal(result)
		contents := string(res)

		// intermediate has type []KeyValue
		intermediate := w.mapf("", contents)
		intermediates = append(intermediates, intermediate...)
	}

	// TODO: fix hardcode
	nreduce := 3
	intermediate_files := make([][]KeyValue, nreduce)

	// sort into their bins
	for _, kv := range intermediates {
		ikey := ihash(kv.Key) % nreduce
		intermediate_files[ikey] = append(intermediate_files[ikey], kv)
	}

	file1 := w.WorkerID.String() + "inter_1.json"
	file2 := w.WorkerID.String() + "inter_2.json"
	file3 := w.WorkerID.String() + "inter_3.json"
	inter_filenames := []string{file1, file2, file3}

	oids := make([]primitive.ObjectID, 0)

	for i, name := range inter_filenames {
		tempFile, err := os.Create(name)
		if err != nil {
			log.Fatal(err)
		}

		// w := io.Writer(tempFile)
		// enc := json.NewEncoder(w)
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate_files[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		tempFile.Close()

		/* open db connection & upload */
		grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
		bucket, err := gridfs.NewBucket(db, grid_opts)
		if err != nil {
			log.Fatal("GridFS NewBucket error: ", err)
		}
		tempFile, _ = os.Open(name)
		objectID, err := bucket.UploadFromStream(name, io.Reader(tempFile))
		if err != nil {
			log.Fatal("bucket.UploadFromStream error: ", err)
		}
		oids = append(oids, objectID)
	}

	for _, file := range inter_filenames {
		os.Remove(file)
	}

	fmt.Println("Worker " + w.WorkerID.String())
	for _, oid := range oids {
		fmt.Println(oid)
	}
	return oids
}

func (w *AviaryWorker) handleReduce(job CoordinatorRequest) {
	fmt.Println("(worker) case: REDUCE")

	// files to download from gridfs
	oids := job.OIDs
	fmt.Println("OIDs")
	for _, oid := range oids {
		fmt.Println(oid)
	}

	keyvalues := make([]KeyValue, 0)

	// download the file contents
	for _, oid := range oids {
		// start new context
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
		grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
		bucket, err := gridfs.NewBucket(db, grid_opts)
		if err != nil {
			panic(err)
		}

		downloadStream, err := bucket.OpenDownloadStream(oid)
		if err != nil {
			panic(err)
		}

		dec := json.NewDecoder(downloadStream)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			keyvalues = append(keyvalues, kv)
		}

		downloadStream.Close()
	}

	// sort
	// kva filled with the values of the first intermediate, need to sort
	// "shuffle" step
	sort.Sort(ByKey(keyvalues))

	filename := "reduce" + strconv.Itoa(job.Partition) + ".json"
	tempFile, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer tempFile.Close()

	// now combine the values of the same keys
	i := 0
	for i < len(keyvalues) {
		j := i + 1
		for j < len(keyvalues) && keyvalues[j].Key == keyvalues[i].Key {
			j++
		}
		coalescedValues := []string{}
		for k := i; k < j; k++ {
			coalescedValues = append(coalescedValues, keyvalues[k].Value)
		}
		reducefOutput := w.reducef(keyvalues[i].Key, coalescedValues)
		// print it into the file
		fmt.Fprintf(tempFile, "%v %v\n", keyvalues[i].Key, reducefOutput)
		i = j
	}

	// remove the *.so file
	os.Remove("tmp" + w.WorkerID.String() + "lib.so")
}

// wait to be assigned a new job from the coordinator
func (w *AviaryWorker) Start() {
	for {
		// await new job
		job := <-w.requestCh
		fmt.Printf("(worker) received new job: %v\n", job)

		// download the map and reduce functions
		w.downloadCh <- job.FunctionID

		// block until the channel
		<-w.startCh

		switch job.Phase {
		case MAP:
			oids := w.handleMap(job)

			// once map task is done, worker needs to notify coordinator
			request := MapCompleteRequest{
				WorkerID: w.WorkerID,
				OIDs:     oids,
			}
			reply := MapCompleteReply{}
			w.callMapComplete(&request, &reply)

			/************************************************************************/

		case REDUCE:
			fmt.Println("(Aviary Worker) case: REDUCE")
			w.handleReduce(job)

			// once reduce task is done, worker needs to notify coordinator
			request := ReduceCompleteRequest{}
			reply := ReduceCompleteReply{}
			w.callReduceComplete(&request, &reply)

		default:
		}
	}
}

// TODO: for workers on startup, workers need to send an RPC to the coordinator
// and provide the coordinator with it's HTTP endpoint
func workerCall(rpcname string, args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var err error
	for {
		// continuously loop until worker can contact Coordinator
		c, err = rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
		if err == nil {
			break
		}
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Fatal(err)
	return false
}

func (w *AviaryWorker) callMapComplete(request *MapCompleteRequest, reply *MapCompleteReply) {
	for {
		ok := workerCall("AviaryCoordinator.MapComplete", request, reply)
		if ok {
			fmt.Printf("Coordinator replied OK to MapComplete RPC\n")
			return
		}
		time.Sleep(time.Second)
	}
}

func (w *AviaryWorker) callReduceComplete(request *ReduceCompleteRequest, reply *ReduceCompleteReply) {
	for {
		ok := workerCall("AviaryCoordinator.ReduceComplete", request, reply)
		if ok {
			fmt.Printf("Coordinator replied OK to ReduceComplete RPC\n")
			return
		}
		time.Sleep(time.Second)
	}
}

// calls the Coordinator's WorkerRequestHandler
func WorkerCall(request *WorkerRequest, reply *WorkerReply) {
	fmt.Println("Entered WorkerCall")
	fmt.Println(*request)

	for {
		ok := workerCall("AviaryCoordinator.WorkerRequestHandler", request, reply)
		if ok {
			fmt.Println("Coordinator replied OK to Worker RPC")
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
	switch request.Phase {
	case MAP:
		fmt.Println("(worker) Pushing Map task on the channel")
		w.requestCh <- *request
		// TODO: bug, immediate return of nil !=> request processed correctly
		reply.Message = OK
		return nil

	case REDUCE:
		fmt.Println("(worker) Pushing Reduce task on the channel")
		w.requestCh <- *request
		reply.Message = OK
		return nil

	default:
		reply.Reply = OK
		return nil
	}
}

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
