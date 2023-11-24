package main

import (
	aviary "aviary/internal"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB connection string
const uri = "mongodb://126.0.0.1:27117,127.0.0.1:27118"

// NOTE: DO NOT RUN AGAIN (don't want duplicates in the database)
func main() {
	// read in the file
	// fileBytes, err := os.ReadFile("pg-being_ernest.txt")
	// fileBytes, err := os.ReadFile("pg-dorian_gray.txt")
	// fileBytes, err := os.ReadFile("pg-frankenstein.txt")
	// fileBytes, err := os.ReadFile("pg-grimm.txt")
	// fileBytes, err := os.ReadFile("pg-huckleberry_finn.txt")
	// fileBytes, err := os.ReadFile("pg-metamorphosis.txt")
	// fileBytes, err := os.ReadFile("pg-sherlock_holmes.txt")
	fileBytes, err := os.ReadFile("pg-tom_sawyer.txt")
	if err != nil {
		log.Fatal(err)
	}
	fileContents := string(fileBytes)
	fileSlice := strings.Fields(fileContents)

	documents := []interface{}{}
	for i := 0; i < len(fileSlice); i += 500 {
		var data aviary.InputData
		if i+500 >= len(fileSlice) {
			s := ""
			for j := i; j < len(fileSlice); j++ {
				s = s + " " + fileSlice[j]
			}
			data = aviary.InputData{
				Tag:       "wc",
				Partition: rand.Intn(3),
				Contents:  s,
			}
		} else {
			s := ""
			for j := i; j < i+500; j++ {
				s = s + " " + fileSlice[j]
			}
			data = aviary.InputData{
				Tag: "wc",
				// partition should really just % number of shards
				Partition: rand.Intn(3),
				Contents:  s,
			}
		}
		documents = append(documents, data)
	}

	// batch insert into MongoDB
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

	// result, err = collection.InsertMany(context.TODO(), documents.([]interface{}))

	collection := client.Database("db").Collection("coll")
	ctxt, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := collection.InsertMany(ctxt, documents)
	if err != nil {
		panic(err)
	}

	fmt.Printf("res: %v\n", res)
}
