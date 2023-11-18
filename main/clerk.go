package main

import (
	aviary "aviary/internal"
	"bufio"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// how the client can interface with aviary through a CLI
func show_help() {
	fmt.Println("To start a MapReduce job, your input should look something like this: ")
	fmt.Printf("\t\t(aviary) ~> begin [your id] [path/to/map.go] [path/to/reduce.go] [database name] [collection name]\n")
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

// reads in local file and converts it into a string
// TODO: this won't work. need to compile it to a *.so file and
// store the *.so file into mongodb instead
//   - but the idea is essentially the same, and we can just store the
//     *.so as a []byte
func fileContents(filename string) string {
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("read file error: ", err)
	}
	return string(fileBytes)
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

			clientID := argv[1]
			mapFunc := argv[2]
			reduceFunc := argv[3]
			dbName := argv[4]
			collName := argv[5]

			request := aviary.ClerkRequest{
				Type:           aviary.JOB,
				ClientID:       aviary.IHash(clientID),
				MapFunc:        fileContents(mapFunc),
				ReduceFunc:     fileContents(reduceFunc),
				DatabaseName:   dbName,
				CollectionName: collName,
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
			clientID := "user"
			mapFunc := "../mrapps/map.go"
			reduceFunc := "../mrapps/reduce.go"
			dbName := "db"
			collName := "coll"
			request := aviary.ClerkRequest{
				Type:           aviary.JOB,
				ClientID:       aviary.IHash(clientID),
				MapFunc:        fileContents(mapFunc),
				ReduceFunc:     fileContents(reduceFunc),
				DatabaseName:   dbName,
				CollectionName: collName,
			}
			reply := aviary.ClerkReply{}
			ClerkCall(&request, &reply)

		default:
			show_help()
		}
	}
}
