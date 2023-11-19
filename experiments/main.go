package main

import (
	"log"
	"plugin"
)

func loadPlugin(filename string) func() {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("could not load plugin %v", filename)
	}
	xmapf, err := p.Lookup("SharedFunction")
	if err != nil {
		log.Fatalf("cannot find SharedFunction in %v", filename)
	}
	mapf := xmapf.(func())
	return mapf
}

func main() {
	// sharedFunction := loadPlugin("lib.so")
	sharedFunction := loadPlugin("../mrapps/test.so")
	sharedFunction()
}
