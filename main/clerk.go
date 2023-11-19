package main

import (
	aviary "aviary/internal"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const uri = "mongodb://126.0.0.1:27117,127.0.0.1:27118"

// how the client can interface with aviary through a CLI
func show_help() {
	fmt.Println("To start a MapReduce job, your input should look something like this: ")
	fmt.Printf("\t\t(aviary) ~> begin [your id] [path/to/funcs.so] [database name] [collection name] [document tag]\n")
}

func show_show() {
	fmt.Println("To use `show`, the command should look like this: ")
	fmt.Printf("\t\t(aviary) ~> show [your identifier]\n")
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// sockname := coordinatorSock()
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

// creates a new job and notifies the coordinator
func ClerkCall(request *aviary.ClerkRequest, reply *aviary.ClerkReply) {
	ok := call("AviaryCoordinator.ClerkRequestHandler", request, reply)
	if !ok {
		log.Fatal("something went wrong with StartJob")
	}

	if reply.Message != "OK" {
		log.Fatal("reply was not OK")
	}
}

// load the application Map and Reduce functions from a plugin file
func checkPlugin(filename string) bool {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("could not load plugin %v", filename)
	}
	_, err = p.Lookup("Map")
	if err != nil {
		return false
	}
	_, err = p.Lookup("Reduce")
	if err != nil {
		return false
	}
	return true
}

// uploads the plugin file to MongoDB GridFS
func uploadPlugin(filename string, clientID int) primitive.ObjectID {
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
	fmt.Println("Aviary Clerk connected to MongoDB!")
	db := client.Database("db")
	grid_opts := options.GridFSBucket().SetName("aviaryFuncs")
	bucket, err := gridfs.NewBucket(db, grid_opts)
	if err != nil {
		log.Fatal("GridFS NewBucket error: ", err)
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("os.Open error: ", err)
	}

	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"clientID", clientID}})
	objectID, err := bucket.UploadFromStream(filename, io.Reader(file), uploadOpts)
	if err != nil {
		log.Fatal("bucket.UploadFromStream error: ", err)
	}
	return objectID
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("(aviary) ~> ")
		cmdString, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		cmdString = strings.TrimSuffix(cmdString, "\n")
		argv := strings.Fields(cmdString)
		command := argv[0]

		switch command {
		case "h", "help", "H", "HELP":
			show_help()

		case "begin", "b", "BEGIN", "B":
			if len(argv) < 6 {
				fmt.Println("Invalid number of args!")
				show_help()
				continue
			}

			id := argv[1]
			clientID := aviary.IHash(id)

			// make sure the *.so file is valid
			filename := argv[2]
			if !checkPlugin(filename) {
				fmt.Printf("WARNING: %s either didnt have correct function types or couldn't be found. Aborting...\n", filename)
				continue
			}

			// upload the *.so file to GridFS
			functionID := uploadPlugin(filename, clientID)

			dbName := argv[3]
			collName := argv[4]
			tag := argv[5]

			request := aviary.ClerkRequest{
				Type:           aviary.JOB,
				ClientID:       clientID,
				DatabaseName:   dbName,
				CollectionName: collName,
				Tag:            tag,
				FunctionID:     functionID,
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)

		case "show", "SHOW", "s", "S":
			if len(argv) < 2 {
				show_show()
				continue
			}
			clientID := argv[1]
			request := aviary.ClerkRequest{
				Type:     "SHOW",
				ClientID: aviary.IHash(clientID),
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)
			fmt.Println(reply)

		// quickly insert something
		case "debug", "d":

			id := "user"
			clientID := aviary.IHash(id)
			filename := "../mrapps/wc.so"
			dbName := "db"
			collName := "coll"
			tag := "wc"

			// make sure the *.so file is valid
			if !checkPlugin(filename) {
				fmt.Printf("WARNING: %s either didnt have correct function types or couldn't be found. Aborting...\n", filename)
				continue
			}

			fmt.Printf("about to upload %s to GridFS\n", filename)
			// upload the *.so file to GridFS
			functionID := uploadPlugin(filename, clientID)

			request := aviary.ClerkRequest{
				Type:           aviary.JOB,
				ClientID:       clientID,
				DatabaseName:   dbName,
				CollectionName: collName,
				Tag:            tag,
				FunctionID:     functionID,
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)

		default:
			show_help()
		}
	}
}
