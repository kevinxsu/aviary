package main

import (
  "log"
  "time"
  "context"
  "fmt"
  "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const uri = "mongodb://localhost:27017"
func main() {
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

  collection := client.Database("test").Collection("nats")
  ctxt, cancel := context.WithTimeout(context.Background(), 5 * time.Second) 
  defer cancel() 
  for i := 0; i < 123; i++ {
    res, _ := collection.InsertOne(ctxt, bson.D{{"v", i}})
    id := res.InsertedID
    fmt.Printf("res id : %s\n", id)
  }


  // sum := 0 

  cur, err := collection.Find(ctxt, bson.D{})
  if err != nil { log.Fatal(err) }
  defer cur.Close(ctxt)

  for cur.Next(ctxt) {
    var result bson.D
    err := cur.Decode(&result)
    if err != nil { log.Fatal(err) }
    // sum += result.(int)
    fmt.Printf("result: %s\n", result)
  }

  if err := cur.Err(); err != nil { 
    log.Fatal(err)
  }
}



/*
import (
	"aviary/internal/log"
	"aviary/internal/scylla"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

func main() {
	logger := log.CreateLogger("info")

  // define which cluster and keyspace to use, and connect to the cluster

	cluster := scylla.CreateCluster(gocql.Quorum, "catalog", "scylla-node1", "scylla-node2", "scylla-node3")
	session, err := gocql.NewSession(*cluster)
	if err != nil {
		logger.Fatal("unable to connect to scylla", zap.Error(err))
	}
	defer session.Close()

	scylla.SelectQuery(session, logger)
	insertQuery(session, logger)
	scylla.SelectQuery(session, logger)
	deleteQuery(session, logger)
	scylla.SelectQuery(session, logger)
}

func insertQuery(session *gocql.Session, logger *zap.Logger) {
	logger.Info("Inserting Mike")
	if err := session.Query("INSERT INTO mutant_data (first_name,last_name,address,picture_location) VALUES ('Mike','Tyson','1515 Main St', 'http://www.facebook.com/mtyson')").Exec(); err != nil {
		logger.Error("insert catalog.mutant_data", zap.Error(err))
	}
}

func deleteQuery(session *gocql.Session, logger *zap.Logger) {
	logger.Info("Deleting Mike")
	if err := session.Query("DELETE FROM mutant_data WHERE first_name = 'Mike' and last_name = 'Tyson'").Exec(); err != nil {
		logger.Error("delete catalog.mutant_data", zap.Error(err))
	}
}
*/
