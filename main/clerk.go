package main

import (
	aviary "aviary/internal"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"plugin"
	"runtime"
	"strconv"
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

func show_status() {
	fmt.Println("To use `status`, the command should look like this: ")
	fmt.Printf("\t\t(aviary) ~> status [your identifier]\n")
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

	if reply.Message != aviary.OK && reply.Message != aviary.NOTREADY {
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

	defer file.Close()
	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"clientID", clientID}})
	objectID, err := bucket.UploadFromStream(filename, io.Reader(file), uploadOpts)
	if err != nil {
		log.Fatal("bucket.UploadFromStream error: ", err)
	}
	return objectID
}

// download the results of a Client's (completed) MapReduce job
func downloadResults(oids []primitive.ObjectID, clientID string, jobID int) {
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
	// need to open a connection to the database
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Aviary Clerk connected to MongoDB!")
	db := client.Database("db")
	grid_opts := options.GridFSBucket().SetName("aviaryResults")
	bucket, err := gridfs.NewBucket(db, grid_opts)
	if err != nil {
		panic(err)
	}

	// create a directory for the resulting .json files
	dirName := clientID + "-job-" + strconv.Itoa(jobID)
	err = os.Mkdir(dirName, 0755)
	if err != nil {
		panic(err)
	}

	for i, oid := range oids {
		tempFileName := dirName + "/result" + strconv.Itoa(i) + ".json"
		tempFile, err := os.Create(tempFileName)
		if err != nil {
			panic(err)
		}
		defer tempFile.Close()

		downloadStream, err := bucket.OpenDownloadStream(oid)
		if err != nil {
			panic(err)
		}
		defer downloadStream.Close()
		_, err = io.Copy(tempFile, downloadStream)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("Results saved to %v\n", dirName)
}

// compile a local ".go" file
func compile(path string) (string, error) {
	fmt.Println("entered compile")
	tmp := aviary.Gensym().String() + ".so"
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", tmp, path)
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	// cmd.Wait()

	out, err := exec.Command("ls").Output()
	if err != nil {
		return "", err
	}
	fmt.Println(string(out))

	if !checkPlugin(tmp) {
		return "", errors.New("invalid map/reduce functions")
	}
	return tmp, nil
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

			filename := argv[2]
			tmp, err := compile(filename)
			if err != nil {
				continue
			}

			// upload the freshly compiled *.so file to GridFS
			functionID := uploadPlugin(tmp, clientID)

			// remove the tmp binaries
			os.Remove(tmp)

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

		case "show", "SHOW", "status", "STATUS", "s", "S":
			if len(argv) < 2 {
				show_status()
				continue
			}
			clientID := argv[1]
			request := aviary.ClerkRequest{
				Type:     "SHOW",
				ClientID: aviary.IHash(clientID),
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)
			aviary.PrettyPrintJobs(reply.Jobs)

		// [get] [user] [job] [job id]
		case "get", "GET", "g", "DOWNLOAD", "download":
			if len(argv) < 4 {
				fmt.Println("usage: get [user] job [i] to download the result locally")
				continue
			}
			clientID := argv[1]
			jobID, err := strconv.Atoi(argv[3])
			if err != nil {
				panic(err)
			}
			request := aviary.ClerkRequest{
				Type:     aviary.GET,
				ClientID: aviary.IHash(clientID),
				JobID:    jobID,
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)
			if reply.Message == aviary.NOTREADY {
				fmt.Println("MR job not ready yet!")
			} else {
				downloadResults(reply.OIDS, clientID, jobID)
			}

		// quickly insert something
		case "debug", "d":

			id := "user"
			clientID := aviary.IHash(id)
			filename := "../mrapps/wc.go"
			dbName := "db"
			collName := "coll"
			tag := "wc"

			var functionID primitive.ObjectID

			if runtime.GOOS != "darwin" {
				tmp, err := compile(filename)
				if err != nil {
					log.Fatal(err)
				}

				fmt.Printf("about to upload %s to GridFS\n", filename)
				functionID = uploadPlugin(tmp, clientID)
				os.Remove(tmp)
			}

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
