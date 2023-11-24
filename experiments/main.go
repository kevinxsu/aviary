package main

import (
	aviary "aviary/internal"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
func loadPlugin(filename string) func() {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("could not load plugin %v", filename)
	}
	xmapf, err := p.Lookup("SharedFunction")
	if err != nil {
		log.Fatalf("cannot find SharedFunction in %v", filename)
	}
	mapf := xmapf.(func())
	return mapf
}

func _main() {
	// sharedFunction := loadPlugin("lib.so")
	sharedFunction := loadPlugin("../mrapps/test.so")
	sharedFunction()
}
*/

type KeyValue aviary.KeyValue

func upload() primitive.ObjectID {
	uri := aviary.URI
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

	// example : encode it into tmp file
	kva := []KeyValue{KeyValue{"a", "1"}, KeyValue{"b", "2"}}
	filename := "TEST_INTERMEDIATE.json"
	file, _ := os.Create(filename)
	// defer file.Close()

	// encode
	for _, kv := range kva {
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	file.Close()

	/* open db connection & upload */
	db := client.Database("db")
	grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
	bucket, err := gridfs.NewBucket(db, grid_opts)
	if err != nil {
		log.Fatal("GridFS NewBucket error: ", err)
	}

	file, _ = os.Open(filename)

	// objectID, err := bucket.UploadFromStream(filename, file)
	objectID, err := bucket.UploadFromStream(filename, io.Reader(file))
	if err != nil {
		panic(err)
	}
	return objectID
}

func download(oid primitive.ObjectID) {
	uri := aviary.URI
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

	/* open db connection & upload */
	grid_opts := options.GridFSBucket().SetName("aviaryIntermediates")
	bucket, err := gridfs.NewBucket(db, grid_opts)
	if err != nil {
		log.Fatal("GridFS NewBucket error: ", err)
	}

	downloadStream, err := bucket.OpenDownloadStream(oid)
	if err != nil {
		panic(err)
	}

	tmpFilename := "tmp.json"
	file, err := os.Create(tmpFilename)
	if err != nil {
		panic(err)
	}

	bytesWritten, err := io.Copy(file, downloadStream)
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote ", bytesWritten)
}

func main() {
	oid := upload()

	download(oid)
}
