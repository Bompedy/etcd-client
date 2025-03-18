package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var numClients, numClientsError = strconv.ParseInt(os.Getenv("NUM_CLIENTS"), 10, 64)
var totalOps, totalOpsError = strconv.ParseInt(os.Getenv("TOTAL_OPS"), 10, 64)
var dataLength, dataLengthError = strconv.ParseInt(os.Getenv("DATA_LENGTH"), 10, 64)
var numThreads, threadsError = strconv.ParseInt(os.Getenv("NUM_THREADS"), 10, 64)
var start = time.Now()

func main() {
	if numClientsError != nil {
		panic(numClientsError)
	}

	if totalOpsError != nil {
		panic(totalOpsError)
	}

	if dataLengthError != nil {
		panic(dataLengthError)
	}

	if threadsError != nil {
		panic(threadsError)
	}

	if numThreads <= 0 {
		numThreads = 1
	}

	connections := make([]*etcdserverpb.KVClient, numClients)
	data := strings.Repeat("a", int(dataLength))
	//completed := make(chan struct{})
	for i := 0; i < int(numClients); i++ {
		connection, err := grpc.Dial(
			"10.10.1.1:2379",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			log.Fatal(err)
		}

		client := etcdserverpb.NewKVClient(connection)
		connections[i] = &client
	}

	var startGroup sync.WaitGroup
	var completeGroup sync.WaitGroup
	startGroup.Add(int(numThreads))
	completeGroup.Add(int(numThreads))

	opsPerThread := int(totalOps / numThreads)
	for i := 0; int64(i) < numThreads; i++ {
		go func(threadID int) {
			connectionIndex := int64(0)
			startOp := threadID * opsPerThread
			startGroup.Done()
			startGroup.Wait()

			for op := range opsPerThread {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				request := &etcdserverpb.PutRequest{
					Key:   []byte(strconv.Itoa(int(op + startOp))),
					Value: []byte(data),
				}
				connectionIndex += 1
				client := *connections[connectionIndex%numClients]
				_, err := client.Put(ctx, request)
				if err != nil {
					log.Fatal(err)
				}
			}
			completeGroup.Done()
		}(i)
	}

	startGroup.Wait()
	start = time.Now()
	completeGroup.Wait()

	total := time.Since(start)
	ops := float64(totalOps) / total.Seconds()
	fmt.Printf("Total clients: %d\n", numClients)
	fmt.Printf("Total threads: %d\n", numThreads)
	fmt.Printf("Total operations: %d\n", totalOps)
	fmt.Printf("Total time taken: %v\n", total)
	fmt.Printf("Data size: %d bytes\n", dataLength)
	fmt.Printf("OPS: %.4f ops/sec\n", ops)
}
