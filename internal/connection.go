package aviary

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// long-running goroutine to maintain a connection between Coordinator and MongoDB
func (c *AviaryCoordinator) mongoConnection(ch chan bool) {
	// set to stable version 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	// create a new client and connect it to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// send ping to confirm successful connection
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	CPrintf("[mongoConnection] coordinator connected to MongoDB!\n")

	// for now, we'll assume that all the users use one database, called "db"
	intermediates := client.Database("db").Collection("aviaryIntermediates")
	functions := client.Database("db").Collection("aviaryFuncs")

	// unblock MakeCoordinator thread
	ch <- true

	// continuously poll for events
	for {
		select {
		case toInsert := <-c.insertCh:
			CPrintf("[mongoConnection] CASE: toInsert := <-c.insertCh\n\n")

			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := functions.InsertOne(ctxt, toInsert)
			if err != nil {
				log.Fatal("mongo insert error: ", err)
			}
			CPrintf("[mongoConnection] Coordinator inserted %v with id %v\n\n", res, res.InsertedID)

		case filter := <-c.findCh:
			CPrintf("[mongoConnection] CASE: filter := <-c.findCh\n\n")
			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := intermediates.Find(ctxt, filter)
			if err != nil {
				log.Fatal("mongo find error: ", err)
			}
			CPrintf("Coordinator found %v\n\n", res)

		// case <-c.createIntCollectionCh:
		// 	CPrintf("[mongoConnection] CASE: <-c.createIntCollectionCh\n\n")
		// 	intermediate := client.Database("db").CreateCollection(context.TODO(), "aviaryIntermediates")
		// 	if err != nil {
		// 		panic(err)
		// 	}

		case <-c.dropCollectionsCh:
			CPrintf("[mongoConnection] CASE: <-c.dropCollectionsCh\n\n")
			for _, name := range []string{
				"aviaryIntermediates.files",
				"aviaryIntermediates.chunks",
			} {
				col := client.Database("db").Collection(name)
				col.Drop(context.TODO())
			}
			c.dropCollectionsResultCh <- true
		}
	}
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
	WPrintf("[mongoConnection] worker successfully connected to MongoDB!\n\n")

	db := client.Database("db")
	collection := db.Collection("coll")
	ch <- true

	for {
		select {
		case filter := <-w.findCh:
			WPrintf("Case: filter := <-w.findCh\n\n")
			ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var res interface{}
			err := collection.FindOne(ctxt, filter).Decode(&res)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					WPrintf("WARNING: Aviary Worker couldn't find documents\n\n")
					continue
				}
				log.Fatal("mongo find error: ", err)
			}
			WPrintf("Worker found %v\n", res)

		case fileID := <-w.downloadCh:
			WPrintf("[mongoConnection] downloading file from GridFS]\n\n")

			if runtime.GOOS != "darwin" {
				grid_opts := options.GridFSBucket().SetName("aviaryFuncs")
				bucket, err := gridfs.NewBucket(db, grid_opts)
				if err != nil {
					WPrintf("failed here1\n")
					panic(err)
				}

				WPrintf("[mongoConnection] opening download stream\n\n")
				downloadStream, err := bucket.OpenDownloadStream(fileID)
				if err != nil {
					WPrintf("failed while trying to open the download stream\n")
					panic(err)
				}

				tmpFilename := "tmp" + w.WorkerID.String() + "lib.so"

				WPrintf("[mongoConnection] creating tmp file: %s\n", tmpFilename)
				file, err := os.Create(tmpFilename)
				if err != nil {
					WPrintf("failed to create the tmp file\n")
					panic(err)
				}

				WPrintf("[mongoConnection] copying downloadStream contents to tmp file\n")
				_, err = io.Copy(file, downloadStream)
				if err != nil {
					WPrintf("failed to copy the file\n")
					panic(err)
				}

				// file downloaded now. need to update worker's map and reduce functions
				WPrintf("[mongoConnection] loading Map/Reduce functions from plugin\n")
				mapf, reducef := loadPlugin(tmpFilename) // TODO BUG: killed here
				w.mapf = mapf
				w.reducef = reducef

				defer downloadStream.Close()
				defer file.Close()
			} else {
				WPrintf("[mongoConnection] skipping download step for macOS")
				w.mapf = Map
				w.reducef = Reduce
			}
			// allow the other goroutine to make progress
			w.startCh <- true

		case filter := <-w.mapCh:
			// find the stuff
			cursor, err := collection.Find(context.TODO(), filter)
			if err != nil {
				panic(err)
			}

			var results []InputData
			if err = cursor.All(context.TODO(), &results); err != nil {
				panic(err)
			}
			w.mapResultsCh <- results

		// upload the intermediates to gridFS
		case name := <-w.intermediatesCh:
			grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
			bucket, err := gridfs.NewBucket(db, grid_opts)
			if err != nil {
				log.Fatal("GridFS NewBucket error: ", err)
			}
			tempFile, _ := os.Open(name)
			objectID, err := bucket.UploadFromStream(name, io.Reader(tempFile))
			if err != nil {
				log.Fatal("bucket.UploadFromStream error: ", err)
			}
			// send back to main goroutine
			w.uploadedCh <- objectID

		// download the intermediates
		case oid := <-w.reduceCh:
			grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
			bucket, err := gridfs.NewBucket(db, grid_opts)
			if err != nil {
				panic(err)
			}

			keyvalues := make([]KeyValue, 0)

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

			w.reduceResultsCh <- keyvalues

		// upload the result of the reduction to gridfs
		case result := <-w.uploadResultsCh:
			grid_opts := options.GridFSBucket().SetName("aviaryResults")
			bucket, err := gridfs.NewBucket(db, grid_opts)
			if err != nil {
				log.Fatal("GridFS NewBucket error: ", err)
			}
			tempFile, _ := os.Open(result.filename)
			defer tempFile.Close()
			objectID, err := bucket.UploadFromStream(result.filename, io.Reader(tempFile))
			if err != nil {
				panic(err)
			}
			w.resultOidCh <- objectID
		}
	}
}
