package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// const uri = "mongodb://localhost:27017"
const uri = "mongodb://127.0.0.1:27117,127.0.0.1:27118"

type PhaseType string

const (
	NONE   PhaseType = "NONE"
	MAP              = "MAP"
	REDUCE           = "REDUCE"
	IDLE             = "IDLE"
	EXIT             = "EXIT"
)

type DataPair struct {
	Key   interface{}
	Value interface{}
}

type WorkerState struct {
	MongoID  primitive.ObjectID `bson:"_id"`
	JobID    int
	WorkerID int
	Phase    PhaseType
	Data     DataPair
}

func _main() {
	// Use the SetServerAPIOptions() method to set the Stable API version to 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// Send a ping to confirm a successful connection
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

	// collection := client.Database("test").Collection("nats")
	// collection := client.Database("test").Collection("nats")
	collection := client.Database("MyDatabase").Collection("aviary-intermediates")
	ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := WorkerState{JobID: 0, WorkerID: 0, Phase: MAP, Data: DataPair{"alfalfa", 0}}
	// res, _ := collection.InsertOne(ctxt, bson.D{ {"JobID", 0}, {"WorkerID", 0}, {"Phase", "MAP"}, {"alfalfa", 0}})

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	dec := gob.NewDecoder(&buffer)

	err = enc.Encode(state)
	if err != nil {
		log.Fatal(err)
	}

	// bson_state, err := bson.Marshal(state)
	// res, _ := collection.InsertOne(ctxt, state)
	// res, err := collection.InsertOne(ctxt, bson_state)
	res, _ := collection.InsertOne(ctxt, buffer)
	if err != nil {
		log.Fatal(err)
	}

	id := res.InsertedID
	fmt.Printf("res id : %s\n", id)

	// res, _ = collection.InsertOne(ctxt, bson.D{
	// 	/* {"state", bson.D{
	// 		{"job", 0},
	// 		{"worker_id", 1},
	// 		{"phase", "map"},
	// 	}},
	// 	{"the", 99}}) */

	// 	{"JobID", 0},
	// 	{"WorkerID", 1},
	// 	{"Phase", "MAP"},
	// 	{"the", 99}})
	// id = res.InsertedID
	// fmt.Printf("res id : %s\n", id)

	// res, _ = collection.InsertOne(ctxt, bson.D{
	// 	/* {"state", bson.D{
	// 		{"job", 0},
	// 		{"worker_id", 2},
	// 		{"phase", "map"},
	// 	}},
	// 	{"the", 1}}) */
	// 	{"JobID", 0},
	// 	{"WorkerID", 2},
	// 	{"Phase", "MAP"},
	// 	{"the", 1}})
	// id = res.InsertedID
	// fmt.Printf("res id : %s\n", id)

	// for i := 0; i < 123; i++ {
	// 	res, _ := collection.InsertOne(ctxt, bson.D{{"vasdfasdfasdfasdfasfsdfasdfsdfasdfasdfasdfasdf", i}})
	// 	id := res.InsertedID
	// 	fmt.Printf("res id : %s\n", id)
	// }

	// // sum := 0

	// coll := collection.FindOne(ctxt, filter).Decode(&result)
	// coll := collection
	// filter := bson.D{{"the", {}}}
	for i := 0; i < 3; i++ {

		// filter := bson.D{{"WorkerID", i}}
		filter := bson.D{}

		cur, err := collection.Find(ctxt, filter)
		if err != nil {
			log.Fatal(err)
		}
		defer cur.Close(ctxt)

		for cur.Next(ctxt) {
			var state WorkerState

			var result bson.D

			err := cur.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}

			err = dec.Decode(&state)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(state)
		}

		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
	}

	/*
		cur, err := collection.Find(ctxt, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		defer cur.Close(ctxt)

		for cur.Next(ctxt) {
			var result bson.D
			err := cur.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			// sum += result.(int)
			fmt.Printf("result: %s\n", result)
		}

		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
	*/
}
