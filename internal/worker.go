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
	"sort"
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

			// fmt.Println(len(intermediates))
			// fmt.Println(intermediates[0])
			// fmt.Println(intermediates)

			// save the intermediates into a file and then insert into GridFS
			// unique filename: worker UUID + ??
			/*
				filename := w.WorkerID.String() + "-" + strconv.Itoa(job.Partition) + ".json"

				tempFile, err := os.Create(filename)

				if err != nil {
					log.Fatal(err)
				}

				// write all of the intermediates to the tempfile
				enc := json.NewEncoder(tempFile)
				for _, kv := range intermediates {
					err := enc.Encode(kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				err = tempFile.Close()
				if err != nil {
					log.Fatal(err)
				}
			*/

			// TODO: fix hardcode
			nreduce := 3
			intermediate_files := make([][]KeyValue, nreduce)

			// sort into their bins
			for _, kv := range intermediates {
				ikey := ihash(kv.Key) % nreduce
				intermediate_files[ikey] = append(intermediate_files[ikey], kv)
			}

			inter_filenames := []string{"inter_map_1.json", "inter_map_2.json", "inter_map_3.json"}

			obj_ids := make([]primitive.ObjectID, 0)

			for i, name := range inter_filenames {
				tempFile, err := os.Create(name)
				if err != nil {
					log.Fatal(err)
				}

				enc := json.NewEncoder(tempFile)
				for _, kv := range intermediate_files[i] {
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				/* open db connection & upload */
				grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
				bucket, err := gridfs.NewBucket(db, grid_opts)
				if err != nil {
					log.Fatal("GridFS NewBucket error: ", err)
				}
				uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{}})
				// objectID, err := bucket.UploadFromStream(filename, io.Reader(tempFile), uploadOpts)
				objectID, err := bucket.UploadFromStream(name, io.Reader(tempFile), uploadOpts)
				if err != nil {
					log.Fatal("bucket.UploadFromStream error: ", err)
				}

				obj_ids = append(obj_ids, objectID)
			}

			fmt.Printf("obj_ids: %v\n", obj_ids)

			// tempFile, err = os.Open(filename)
			// if err != nil {
			// 	log.Fatal(err)
			// }

			/************************************************************************/

			// once map task is done, worker needs to notify coordinator
			request := WorkerRequest{
				WorkerID:    w.WorkerID,
				WorkerState: MAP_DONE,
				OIDs:        obj_ids,
			}

			reply := WorkerReply{}

			// go?
			WorkerCall(&request, &reply)

			/************************************************************************/

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

/*
func (w *AviaryWorker) ApplyReduce() {
	tempFile, err := ioutil.TempFile("./", getRandomName())
	defer tempFile.Close()
	if err != nil {
		log.Fatal(err)
		panic("Worker.go: ApplyReduce(): Error in creating temp file!\n")
	}

	intermediate := []KeyValue{}
	// iterate over all intermediate files
	for _, filename := range filenames {
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// kva filled with the values of the first intermediate, need to sort
	// "shuffle" step
	sort.Sort(ByKey(intermediate))

	// now combine the values of the same keys
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		coalescedValues := []string{}
		for k := i; k < j; k++ {
			coalescedValues = append(coalescedValues, intermediate[k].Value)
		}
		reducefOutput := w.reducef(intermediate[i].Key, coalescedValues)
		// print it into the file
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, reducefOutput)
		i = j
	}

	filename := fmt.Sprintf("mr-out-%d", taskID)
	os.Rename(tempFile.Name(), filename)

	return filename
}

*/

// Worker's RPC handler for Coordinator notifications
func (w *AviaryWorker) CoordinatorRequestHandler(request *CoordinatorRequest, reply *CoordinatorReply) error {
	fmt.Println("(worker) Entered CoordinatorRequestHandler")
	fmt.Println(*request)
	switch request.Phase {
	case MAP:
		fmt.Println("(worker) case: MAP")
		w.requestCh <- *request
		// TODO: bug, immediate return of nil !=> request processed correctly
		reply.Message = OK
		return nil

	case REDUCE:
		fmt.Println("(worker) case: REDUCE")

		// files to download from gridfs
		oids := request.OIDs
		fmt.Printf("oids: %v\n", oids)

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
			db := client.Database(request.DatabaseName)
			// collection := db.Collection(job.CollectionName)

			// grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
			grid_opts := options.GridFSBucket().SetName("aviaryIntermediates.files")
			bucket, err := gridfs.NewBucket(db, grid_opts)
			if err != nil {
				log.Fatal(err)
			}

			downloadStream, err := bucket.OpenDownloadStream(oid)
			if err != nil {
				log.Fatal(err)
				panic(err)
			}

			tmpFilename := "tmp" + w.WorkerID.String() + ".json"
			file, err := os.Create(tmpFilename)
			if err != nil {
				log.Fatal(err)
				panic(err)
			}

			_, err = io.Copy(file, downloadStream)
			if err != nil {
				log.Fatal(err)
				panic(err)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					log.Fatal(err)
					break
				}
				keyvalues = append(keyvalues, kv)
			}

			// bytes, err = ioutil.ReadAll(file)
			// if err != nil {
			// 	panic(err)
			// }
			// json.Unmarshal()

			defer downloadStream.Close()
			defer file.Close()
		}

		// []
		fmt.Println(keyvalues)

		// sort
		// kva filled with the values of the first intermediate, need to sort
		// "shuffle" step
		sort.Sort(ByKey(keyvalues))

		filename := "reduce" + strconv.Itoa(request.Partition) + ".json"
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

		reply.Reply = OK
		return nil

	default:
		reply.Reply = OK
		return nil
	}
}
