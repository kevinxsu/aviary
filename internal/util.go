package aviary

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type UUID = uuid.UUID

func Gensym() UUID {
	return uuid.New()
}

func (c *AviaryCoordinator) _gensym() UUID {
	return uuid.New()
}

func (c *AviaryCoordinator) gensym() UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c._gensym()
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func IHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// pretty print the jobs (no locking)
func (c *AviaryCoordinator) _prettyPrintJobs() {
	for clientID, jobs := range c.jobs {
		fmt.Printf("Client %d:\n", clientID)
		for _, job := range jobs {
			fmt.Printf("\t%v\n", job)
		}
	}
}

// random string generator for temporary file names
func getRandomName() string {
	rand.Seed(time.Now().UnixNano())
	digits := "0123456789"
	all := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		digits
	length := 8
	buf := make([]byte, length)
	buf[0] = digits[rand.Intn(len(digits))]
	for i := 1; i < length; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return string(buf)
}
