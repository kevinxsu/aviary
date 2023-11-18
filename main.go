package main

import (
	aviary "aviary/internal"
	"fmt"
)

func main() {
	// Your code here
	uuid := aviary.Gensym()
	fmt.Println(uuid)

	c := aviary.MakeCoordinator()

	fmt.Println(c.GetJobs())
}
