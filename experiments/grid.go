package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const _uri = "mongodb://126.0.0.1:27117,127.0.0.1:27118"

func main() {
	fmt.Println("ok")

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(_uri).SetServerAPIOptions(serverAPI)
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
	grid_opts := options.GridFSBucket().SetName("aviaryFuncs")
	bucket, err := gridfs.NewBucket(db, grid_opts)

	// opts := options.GridFSBucket().SetName("aviaryFuncs")
	// bucket, err := gridfs.NewBucket(db, opts)
	if err != nil {
		panic(err)
	}

	file, err := os.Open("lib.so")
	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"metadata tag", "first"}})
	objectID, err := bucket.UploadFromStream("lib.so", io.Reader(file), uploadOpts)

	if err != nil {
		panic(err)
	}

	fmt.Printf("New file uploaded with ID %s\n", objectID)

	// fmt.Println(bucket)

	// db := client.Database("db")
	// opts := options.GridFSBucket().SetName("aviaryFuncs")
	// // collection := db.Collection("coll")

	// bucket, err := gridfs.NewBucket(db, opts)
	// if err != nil {
	// 	panic(err)
	// }

}
