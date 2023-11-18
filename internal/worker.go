package aviary

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

func WorkerCall(request *Request, reply *Reply) {
	ok := call("Coordinator.RequestHandler", request, reply)
	if !ok {
		os.Exit(0)
	}
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
