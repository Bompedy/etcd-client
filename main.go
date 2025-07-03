package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var completedOps uint32

type Client struct {
	Addresses      []string
	TotalAddresses int
	DataSize       int
	NumOps         int
	ReadRatio      float64
	NumClients     int
	NumClientOps   int

	Clients      []etcdserverpb.KVClient
	Keys         [][]byte
	WarmupValues [][]byte
	UpdateValues [][]byte
}

func main() {
	addresses := strings.Split(os.Args[1], ",")
	totalAddresses := len(addresses)
	dataSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	numOps, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	readRatio, err := strconv.ParseFloat(os.Args[4], 32)
	if err != nil {
		panic(err)
	}

	numClients, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	numClientOps := numOps / (numClients * totalAddresses)
	client := &Client{
		Addresses:      addresses,
		TotalAddresses: totalAddresses,
		DataSize:       dataSize,
		NumOps:         numClientOps * numClients * totalAddresses,
		ReadRatio:      readRatio,
		NumClients:     numClients,
		NumClientOps:   numClientOps,
		Clients:        make([]etcdserverpb.KVClient, numClients*totalAddresses),
		Keys:           make([][]byte, numOps),
		WarmupValues:   make([][]byte, numOps),
		UpdateValues:   make([][]byte, numOps),
	}

	for i := 0; i < numOps; i++ {
		client.Keys[i] = []byte(fmt.Sprintf("key%d", i))
		client.WarmupValues[i] = []byte(strings.Repeat("x", dataSize))
		client.UpdateValues[i] = []byte(strings.Repeat("z", dataSize))
	}

	client.connect()
	client.warmup()
	client.benchmark()
}

func (client *Client) connect() {
	connected := 0
	for i := range client.TotalAddresses {
		for range client.NumClients {
			connection, err := grpc.Dial(
				client.Addresses[i],
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err != nil {
				panic(err)
			}

			kvClient := etcdserverpb.NewKVClient(connection)
			client.Clients[connected] = kvClient
			connected++
		}
	}

	if connected != client.NumClients*client.TotalAddresses {
		panic(fmt.Sprintf("%d clients connected", connected))
	}
}

func (client *Client) benchmark() {
	var benchmarkBar sync.WaitGroup
	benchmarkBar.Add(1)
	completedOps = 0
	go func() {
		defer benchmarkBar.Done()
		progressBar("Benchmark", client.NumOps)
	}()

	clientReadTimes := make([]int, client.NumOps)
	clientWriteTimes := make([]int, client.NumOps)
	clientTimes := make([]int, client.NumOps)
	group := sync.WaitGroup{}
	group.Add(client.NumClients * client.TotalAddresses)

	start := time.Now().UnixMilli()

	var count uint32
	var readCount uint32
	var writeCount uint32

	for i := range client.Clients {
		go func(i int, kvClient etcdserverpb.KVClient) {
			for c := range client.NumClientOps {

				isRead := rand.Float64() < client.ReadRatio
				key := client.Keys[i*client.NumClientOps+c]
				//warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				updateValue := client.UpdateValues[i*client.NumClientOps+c]
				//value := warmupValue
				begin := time.Now().UnixMicro()

				if !isRead {
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					request := &etcdserverpb.PutRequest{
						Key:   key,
						Value: updateValue,
					}

					_, err := kvClient.Put(ctx, request)
					if err != nil {
						log.Fatal(err)
					}
					cancel()
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					request := &etcdserverpb.RangeRequest{
						Key: key,
					}
					_, err := kvClient.Range(ctx, request)
					if err != nil {
						panic(err)
					}
					//
					//if response.Count != 1 {
					//	panic(fmt.Sprintf("Read operation failed for key %s with count %d", string(key), response.Count))
					//}
					//
					//if !bytes.Equal(response.Kvs[0].Value, value) {
					//	panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s", string(key), string(value), string(response.Kvs[0].Value)))
					//}

					cancel()
				}

				end := time.Now().UnixMicro()

				nextCount := atomic.AddUint32(&count, 1)
				clientTimes[nextCount-1] = int(end - begin)

				if isRead {
					nextReadCount := atomic.AddUint32(&readCount, 1)
					clientReadTimes[nextReadCount-1] = int(end - begin)
				} else if !isRead {
					nextWriteCount := atomic.AddUint32(&writeCount, 1)
					clientWriteTimes[nextWriteCount-1] = int(end - begin)
				}
				atomic.AddUint32(&completedOps, 1)
			}
			group.Done()
		}(i, client.Clients[i])
	}

	group.Wait()

	end := time.Now().UnixMilli()
	benchmarkBar.Wait()
	displayResults(start, end, clientTimes, clientWriteTimes, clientReadTimes, int(count), int(writeCount), int(readCount))
}

func (client *Client) warmup() {
	warmup := sync.WaitGroup{}
	warmup.Add(client.NumClients * client.TotalAddresses)

	var warmupBar sync.WaitGroup
	warmupBar.Add(1)
	go func() {
		defer warmupBar.Done()
		progressBar("Warmup", client.NumOps+(client.NumClients*client.TotalAddresses))
	}()

	for i := range client.Clients {
		go func(i int, kvClient etcdserverpb.KVClient) {
			for c := range client.NumClientOps {
				key := client.Keys[i*client.NumClientOps+c]
				warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				value := warmupValue

				if client.Clients[i] == nil {
					panic("Why is the client null?")
				}

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				request := &etcdserverpb.PutRequest{
					Key:   key,
					Value: value,
				}

				_, err := kvClient.Put(ctx, request)
				if err != nil {
					log.Fatal(err)
				}
				cancel()

				atomic.AddUint32(&completedOps, 1)
			}

			key := client.Keys[0]
			//_ := client.WarmupValues[0]

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			request := &etcdserverpb.RangeRequest{
				Key: key,
			}
			_, err := kvClient.Range(ctx, request)
			if err != nil {
				panic(err)
			}

			//if response.Count != 1 {
			//	panic(fmt.Sprintf("Read operation failed for key %s with count %d", string(key), response.Count))
			//}

			//if !bytes.Equal(response.Kvs[0].Value, warmupValue) {
			//	panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s", string(key), string(warmupValue), string(response.Kvs[0].Value)))
			//}

			cancel()

			atomic.AddUint32(&completedOps, 1)

			warmup.Done()
		}(i, client.Clients[i])
	}

	warmup.Wait()
	warmupBar.Wait()
}

func displayResults(
	start int64,
	end int64,
	clientTimes []int,
	clientWriteTimes []int,
	clientReadTimes []int,
	count int,
	writeCount int,
	readCount int,
) {
	fmt.Printf("Write count: %d\n", writeCount)
	fmt.Printf("Read count: %d\n", readCount)
	fmt.Printf("Total count: %d\n", count)

	sort.Ints(clientTimes[:count])
	sort.Ints(clientWriteTimes[:writeCount])
	sort.Ints(clientReadTimes[:readCount])

	avgAll := 0
	maxAll := math.MinInt32
	minAll := math.MaxInt32
	minIndex := 0
	for i := range count {
		timeAll := clientTimes[i]
		avgAll += timeAll
		if timeAll > maxAll {
			maxAll = timeAll
		}
		if timeAll < minAll {
			minAll = timeAll
			minIndex = i
		}
	}
	fmt.Printf("Read Min: %d\n", minIndex)

	avgAll /= count

	avgWrite := 0
	maxWrite := math.MinInt32
	minWrite := math.MaxInt32
	for i := range writeCount {
		timeWrite := clientWriteTimes[i]
		avgWrite += timeWrite
		if timeWrite > maxWrite {
			maxWrite = timeWrite
		}
		if timeWrite < minWrite {
			minWrite = timeWrite
			minIndex = i
		}
	}

	fmt.Printf("Write Min: %d\n", minIndex)
	if writeCount > 0 {
		avgWrite /= writeCount
	}

	avgRead := 0
	maxRead := math.MinInt32
	minRead := math.MaxInt32
	for i := range readCount {
		timeRead := clientReadTimes[i]
		avgWrite += timeRead
		if timeRead > maxRead {
			maxRead = timeRead
		}
		if timeRead < minRead {
			minRead = timeRead
		}
	}
	if readCount > 0 {
		avgRead /= readCount
	}

	if writeCount > 0 && readCount > 0 {
		fmt.Printf("\nAll - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			count, int(float32(count)/(float32(end-start)/1000.0)), avgAll, minAll, maxAll,
			clientTimes[int(float32(count)*0.5)],
			clientTimes[int(float32(count)*0.9)],
			clientTimes[int(float32(count)*0.95)],
			clientTimes[int(float32(count)*0.99)],
			clientTimes[int(float32(count)*0.999)],
			clientTimes[int(float32(count)*0.9999)],
		)
	}
	if writeCount > 0 {
		fmt.Printf("\nUpdate - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			writeCount, int(float32(writeCount)/(float32(end-start)/1000.0)), avgWrite, minWrite, maxWrite,
			clientWriteTimes[int(float32(writeCount)*0.5)],
			clientWriteTimes[int(float32(writeCount)*0.9)],
			clientWriteTimes[int(float32(writeCount)*0.95)],
			clientWriteTimes[int(float32(writeCount)*0.99)],
			clientWriteTimes[int(float32(writeCount)*0.999)],
			clientWriteTimes[int(float32(writeCount)*0.9999)],
		)
	}
	if readCount > 0 {
		fmt.Printf("\nRead - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			readCount, int(float32(readCount)/(float32(end-start)/1000.0)), avgRead, minRead, maxRead,
			clientReadTimes[int(float32(readCount)*0.5)],
			clientReadTimes[int(float32(readCount)*0.9)],
			clientReadTimes[int(float32(readCount)*0.95)],
			clientReadTimes[int(float32(readCount)*0.99)],
			clientReadTimes[int(float32(readCount)*0.999)],
			clientReadTimes[int(float32(readCount)*0.9999)],
		)
	}
}

func progressBar(title string, numOps int) {
	barWidth := 50
	for {
		completed := atomic.LoadUint32(&completedOps)

		if completed > uint32(numOps) {
			completed = uint32(numOps)
		}

		percent := float64(completed) / float64(numOps) * 100
		filled := int(float64(barWidth) * float64(completed) / float64(numOps))
		if filled > barWidth {
			filled = barWidth
		}
		if filled < 0 {
			filled = 0
		}

		bar := "[" + strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled) + "]"
		fmt.Printf("\r%s Progress: %s %.2f%% (%d/%d)", title, bar, percent, completed, numOps)
		if completed >= uint32(numOps) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}
