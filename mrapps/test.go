package main

import (
	aviary "aviary/internal"
	"fmt"
)

type KeyValue = aviary.KeyValue

func Map(filename string, contents string) []KeyValue {
	fmt.Println("Map")
	return []KeyValue{KeyValue{Key: "key", Value: "value"}}
}

func Reduce(key string, values []string) string {
	fmt.Println("Reduce")
	return "Reduce"
}

func SharedFunction() {
	fmt.Println("SharedFunction")
}
