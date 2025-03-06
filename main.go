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
	"sync/atomic"
	"time"
)

var numClients, numClientsError = strconv.ParseInt(os.Getenv("NUM_CLIENTS"), 10, 64)
var totalOps, totalOpsError = strconv.ParseInt(os.Getenv("TOTAL_OPS"), 10, 64)
var dataLength, dataLengthError = strconv.ParseInt(os.Getenv("DATA_LENGTH"), 10, 64)
var sentOps = int64(0)
var completedOps = int64(0)
var start = time.Now()
var endpoints = []string{"10.10.1.1:2379"}

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

	connections := make([]*etcdserverpb.KVClient, numClients)
	data := strings.Repeat("a", int(dataLength))
	completed := make(chan struct{})
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

	go func() {
		start = time.Now()
		for {
			sentOps += 1
			if sentOps > totalOps {
				break
			}

			go func(index int64) {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				request := &etcdserverpb.PutRequest{
					Key:   []byte(strconv.Itoa(int(index))),
					Value: []byte(data),
				}
				client := *connections[index%numClients]
				_, err := client.Put(ctx, request)
				if err != nil {
					log.Fatal(err)
				}

				if atomic.AddInt64(&completedOps, int64(1)) == totalOps {
					close(completed)
				}
			}(sentOps)
		}
	}()

	<-completed
	total := time.Since(start)
	ops := float64(totalOps) / total.Seconds()

	fmt.Printf("Total clients: %d\n", numClients)
	fmt.Printf("Total operations: %d\n", totalOps)
	fmt.Printf("Total time taken: %v\n", total)
	fmt.Printf("Data size: %d bytes\n", dataLength)
	fmt.Printf("OPS: %.4f ops/sec\n", ops)
}
