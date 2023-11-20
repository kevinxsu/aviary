package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

	// file, err := os.Open("lib.so")
	file, err := os.Open("asdf.txt")
	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"metadata tag", "first"}})
	// objectID, err := bucket.UploadFromStream("lib.so", io.Reader(file), uploadOpts)
	objectID, err := bucket.UploadFromStream("asdf.txt", io.Reader(file), uploadOpts)

	if err != nil {
		panic(err)
	}

	fmt.Printf("New file uploaded with ID %s\n", objectID)

	filter := bson.D{{}}
	cursor, err := bucket.Find(filter)
	if err != nil {
		panic(err)
	}

	type gridfsFile struct {
		Name   string `bson:"filename"`
		Length int64  `bson:"length"`
	}

	var foundFiles []gridfsFile
	if err = cursor.All(context.TODO(), &foundFiles); err != nil {
		panic(err)
	}

	for _, file := range foundFiles {
		fmt.Printf("filename: %s, length: %d\n", file.Name, file.Length)
	}

	fmt.Println("GOING OT TRY TO DOWNLOAD FILE FROM GRIDFS")

	oid, _ := primitive.ObjectIDFromHex("655b18d4a41f8227cf117fac")

	// downloadStream, err := bucket.OpenDownloadStream(objectID)

	downloadStream, err := bucket.OpenDownloadStream(oid)
	if err != nil {
		panic(err)
	}

	// dfile, err := os.Create("lib.so")
	dfile, err := os.Create("downloaded_asdf.txt")
	if err != nil {
		panic(err)
	}

	_, err = io.Copy(dfile, downloadStream)
	if err != nil {
		panic(err)
	}

	defer downloadStream.Close()
	defer dfile.Close()

	fmt.Println("downloaded a file from GridFS")

	// id, err := primitive.ObjectIDFromHex()

	// fmt.Println(bucket)

	// db := client.Database("db")
	// opts := options.GridFSBucket().SetName("aviaryFuncs")
	// // collection := db.Collection("coll")

	// bucket, err := gridfs.NewBucket(db, opts)
	// if err != nil {
	// 	panic(err)
	// }

}
