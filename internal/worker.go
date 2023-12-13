package aviary

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TODO: close files after each run? (or just in general)

type AviaryWorker struct {
	WorkerID   UUID       // the worker's identifier
	InProgress []Job      // inprogress jobs (want to allow for concurrent jobs)
	Completed  []Job      // completed jobs
	mu         sync.Mutex // for concurrent requests

	requestCh       chan CoordinatorRequest // channel for incoming jobs sent by the coordinator
	findCh          chan bson.D             // channel for worker to find stuff? (NOT SURE IF THIS EVEN WORKS OR IS THE BEST WAY)
	downloadCh      chan primitive.ObjectID // download
	startCh         chan bool
	mapCh           chan bson.D
	mapResultsCh    chan []InputData
	reduceCh        chan primitive.ObjectID
	reduceResultsCh chan []KeyValue
	uploadResultsCh chan ReducedResults
	resultOidCh     chan primitive.ObjectID

	client *mongo.Client
	fmlCh  chan [][]KeyValue

	// channel to upload intermediate file
	intermediatesCh chan string
	uploadedCh      chan primitive.ObjectID

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

		mapCh:           make(chan bson.D),
		mapResultsCh:    make(chan []InputData),
		intermediatesCh: make(chan string),
		uploadedCh:      make(chan primitive.ObjectID),
		reduceCh:        make(chan primitive.ObjectID),
		reduceResultsCh: make(chan []KeyValue),
		uploadResultsCh: make(chan ReducedResults),
		resultOidCh:     make(chan primitive.ObjectID),

		fmlCh: make(chan [][]KeyValue),

		mapf:    nil,
		reducef: nil,

		port: -1,
	}

	// establish connection with mongodb first
	ch := make(chan bool)
	go w.mongoConnection(ch)

	// serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	// opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)
	// client, err := mongo.Connect(context.TODO(), opts)
	// if err != nil {
	// 	panic(err)
	// }
	// w.client = client
	<-ch

	// start listening for RPCs
	w.server()

	// try to send RPC to coordinator to notify it of our port
	w.callRegisterWorker()

	return &w
}

// TODO: for workers on startup, workers need to send an RPC to the coordinator
// and provide the coordinator with it's HTTP endpoint

// continuously try to send RPCs to the coordinator to subscribe to messages?
func (w *AviaryWorker) callRegisterWorker() {
	request := RegisterWorkerRequest{
		WorkerID:   w.WorkerID,
		WorkerPort: w.port,
	}
	reply := RegisterWorkerReply{}
	// keep trying to send until coordinator receives RPC
	for {
		ok := callRPCWithRetry("AviaryCoordinator.RegisterWorker", &request, &reply, "127.0.0.1", 1234)
		if ok {
			break
		}
		time.Sleep(time.Second)
	}
	WPrintf("[callRegisterWorker] worker %v successfully subscribed to Coordinator!\n\n", w.WorkerID)
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

func (w *AviaryWorker) handleMap(job CoordinatorRequest) []primitive.ObjectID {
	WPrintf("[handleMap] Worker starting to process Map task\n\n")

	// filter by partition number
	filter := bson.D{{"partition", job.Partition}}

	// send the find filter to the channel (in connection.go)
	w.mapCh <- filter

	// get the results of the database find
	results := <-w.mapResultsCh

	// NEW
	imap := make(map[KeyValue][]string)

	// loop over results to aggregate same keyvalues
	for _, result := range results {
		res, _ := json.Marshal(result)
		contents := string(res)
		inter := w.mapf("", contents)
		for _, kv := range inter {
			imap[kv] = append(imap[kv], kv.Value)
		}
	}

	actual_results := make([]KeyValue, 0)
	for kv, kvslice := range imap {
		fmt.Printf("sending to eager reduce %v\n", kvslice)
		reducef_kv := w.reducef(kv.Key, kvslice)
		actual_results = append(actual_results, KeyValue{Key: kv.Key, Value: reducef_kv})
	}

	intermediates := actual_results

	fmt.Printf("%v\n", imap[KeyValue{Key: "A", Value: "1"}])
	fmt.Printf("len: %v\n", len(imap[KeyValue{Key: "A", Value: "1"}]))
	// time.Sleep(60 * time.Second)

	///

	// intermediates := make([]KeyValue, 0)
	// for _, result := range results {
	// 	res, _ := json.Marshal(result)
	// 	contents := string(res)
	// 	// intermediate has type []KeyValue
	// 	intermediate := w.mapf("", contents)

	// 	// TODO: apply special update/reduce logic here
	// 	intermediates = append(intermediates, intermediate...)
	// }

	/******/
	// TODO: fix hardcode
	nreduce := 3
	intermediate_files := make([][]KeyValue, nreduce)

	// sort into their bins
	for _, kv := range intermediates {
		ikey := ihash(kv.Key) % nreduce

		// TODO: maybe change this here
		intermediate_files[ikey] = append(intermediate_files[ikey], kv)

		// NEW STUFF
		// open new connection to DB intermedciates collection

		// check if key exists in collection
		//	if so, then apply update to existing key
		//  else, add the new kv pair directly into the collection

		// intermediates:
		//	 ikey
		//   ikey
		//   ikey
	}
	/////////////////////////////////////////////

	// input the partitioned files into new worker collections
	w.fmlCh <- intermediate_files

	//

	/////////////////////////////////////////////

	// file1 := w.WorkerID.String() + "inter_1.json"
	// file2 := w.WorkerID.String() + "inter_2.json"
	// file3 := w.WorkerID.String() + "inter_3.json"
	// inter_filenames := []string{file1, file2, file3}

	oids := make([]primitive.ObjectID, 0)

	// for i, name := range inter_filenames {
	// 	tempFile, err := os.Create(name)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	defer os.Remove(name)

	// 	enc := json.NewEncoder(tempFile)
	// 	for _, kv := range intermediate_files[i] {
	// 		err = enc.Encode(&kv)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 	}
	// 	tempFile.Close()

	// 	/******/
	// 	// send the name of the file to the channel
	// 	w.intermediatesCh <- name

	// 	// get the objectID of the uploaded file and append to oids
	// 	objectID := <-w.uploadedCh

	// 	/* this line is what we are changing */
	// 	oids = append(oids, objectID)
	// }
	// WPrintf("[handleMap] Worker %v uploaded results to OIDs %v\n", w.WorkerID, oids)
	return oids
}

func (w *AviaryWorker) handleReduce(job *CoordinatorRequest) primitive.ObjectID {
	WPrintf("[handleReduce] Worker starting to process Reduce task\n\n")

	///////////////////////////////////// NEW

	// the data is just in "intermediatesPartition"+strconv.Itoa(job.Partition)

	keyvalues := make([]KeyValue, 0)
	db := w.client.Database("db")
	res, err := db.Collection("intermediatesPartition"+strconv.Itoa(job.Partition)).Find(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal("[handleReduce] Could not find ")
	}

	for res.Next(context.TODO()) {
		var keyval KeyValue
		if err := res.Decode(&keyval); err != nil {
			panic(err)
		}
		fmt.Printf("read %v\n", keyval)
		keyvalues = append(keyvalues, keyval)
	}

	for _, v := range keyvalues {
		if v.Key == "A" {
			fmt.Printf("found %v\n", v)
		}
	}
	// panic("asdf")

	/////////////////////////////////////

	// files to download from gridfs
	// oids := job.OIDs
	// WPrintf("[handleReduce] processing OIDs: %v\n\n", oids)

	// keyvalues := make([]KeyValue, 0)
	// keyvalues = make([]KeyValue, 0)

	// download the file contents
	//	for _, oid := range oids {
	// send the oid to the channel to get the keyvalues associated with it
	//w.reduceCh <- oid
	// 	kvs := <-w.reduceResultsCh
	// 	keyvalues = append(keyvalues, kvs...)
	// }

	// sort kva filled with the values of the first intermediate, need to sort
	// "shuffle" step
	sort.Sort(ByKey(keyvalues))

	filename := strconv.Itoa(job.ClientID) + "_" + strconv.Itoa(job.JobID) + "_" + strconv.Itoa(job.Partition) + ".json"

	tempFile, err := os.Create(filename)
	// tempFile, err := os.CreateTemp("", filename)
	if err != nil {
		log.Fatal(err)
	}
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

	// so scuffed
	tempFile.Close()

	// send the results through the channel, which will upload it to GridFS
	// for the client to later download
	w.uploadResultsCh <- ReducedResults{
		filename:  filename,
		jobID:     job.JobID,
		clientID:  job.ClientID,
		partition: job.Partition,
	}

	oid := <-w.resultOidCh

	// remove the *.so file
	os.Remove("tmp" + w.WorkerID.String() + "lib.so")
	defer os.Remove(filename)
	return oid
}

// wait to be assigned a new job from the coordinator
func (w *AviaryWorker) Start() {
	for {
		// await new job
		job := <-w.requestCh
		WPrintf("[Start] Worker received new job: %v\n\n", job)

		/* bug here
		// download the map and reduce functions
		w.downloadCh <- job.FunctionID

		// block until the channel
		<-w.startCh
		*/

		switch job.Phase {

		/*********************** MAP CASE *****************************************/
		case MAP:
			WPrintf("[Start] Worker in MAP case with job: %v\n\n", job)
			w.downloadCh <- job.FunctionID
			<-w.startCh

			oids := w.handleMap(job)

			// once map task is done, worker needs to notify coordinator
			request := MapCompleteRequest{
				ClientID: job.ClientID,
				JobID:    job.JobID,
				WorkerID: w.WorkerID,
				OIDs:     oids,
			}
			reply := MapCompleteReply{}
			w.callMapComplete(&request, &reply)

		/********************* REDUCE CASE *****************************************/
		case REDUCE:
			WPrintf("[Start] Worker in REDUCE case with job: %v\n\n", job)
			oid := w.handleReduce(&job)

			// once reduce task is done, worker needs to notify coordinator
			request := ReduceCompleteRequest{
				JobID:    job.JobID,
				ClientID: job.ClientID,
				OID:      oid,
			}
			reply := ReduceCompleteReply{}
			w.callReduceComplete(&request, &reply)

		default:
			WPrintf("[Start] Worker in default case ?????????????????????? \n\n")
		}
	}
}

func (w *AviaryWorker) callMapComplete(request *MapCompleteRequest, reply *MapCompleteReply) {
	WPrintf("[callMapComplete] Worker %v about to ping Coordinator, done with MAP\n\n", w.WorkerID)
	for {
		ok := callRPCWithRetry("AviaryCoordinator.MapComplete", request, reply, "127.0.0.1", 1234)
		if ok {
			WPrintf("[callMapComplete] Coordinator replied OK to Worker MapComplete RPC\n\n")
			return
		}
		WPrintf("[callMapComplete] Worker retrying MapComplete RPC to Coordinator\n\n", w.WorkerID)
		time.Sleep(time.Second)
	}
}

func (w *AviaryWorker) callReduceComplete(request *ReduceCompleteRequest, reply *ReduceCompleteReply) {
	WPrintf("[callReduceComplete] Worker %v about to ping Coordinator, done with REDUCE\n\n", w.WorkerID)
	for {
		ok := callRPCWithRetry("AviaryCoordinator.ReduceComplete", request, reply, "127.0.0.1", 1234)
		if ok {
			WPrintf("[callMapComplete] Coordinator replied OK to Worker ReduceComplete RPC\n\n")
			return
		}
		WPrintf("[callMapComplete] Worker retrying ReduceComplete RPC to Coordinator\n\n", w.WorkerID)
		time.Sleep(time.Second)
	}
}

// potential issue: how does coordinator broadcast to all workers?  dont think we can reuse the same port
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

	WPrintf("[server] Worker %v found good port: %d\n", w.WorkerID, w.port)
	go http.Serve(l, nil)
}

// Worker's RPC handler for Coordinator notifications
func (w *AviaryWorker) CoordinatorRequestHandler(request *CoordinatorRequest, reply *CoordinatorReply) error {
	WPrintf("[CoordinatorRequestHandler] received RPC from Coordinator\n\n")
	switch request.Phase {
	case MAP:
		WPrintf("[CoordinatorRequestHandler] pushing Map task on requestCh\n\n")
		w.requestCh <- *request
		// TODO: bug, immediate return of nil !=> request processed correctly
		reply.Message = OK
		return nil

	case REDUCE:
		WPrintf("[CoordinatorRequestHandler] pushing Reduce task on requestCh\n\n")
		w.requestCh <- *request
		reply.Message = OK
		return nil

	default:
		WPrintf("[CoordinatorRequestHandler] default ????????????????????????\n\n")
		reply.Reply = OK
		return nil
	}
}

type WorkerRequest struct {
	JobID       int // the ID of the client's MapReduce job
	ClientID    int // the client's ID is also just an int
	WorkerID    UUID
	WorkerState Phase
	WorkerPort  int                  // if INIT Phase, should only be WorkerID, WorkerState, and WorkerPort
	OIDs        []primitive.ObjectID // the id of the intermediate data from Map phase (stored in GridFS)
}
