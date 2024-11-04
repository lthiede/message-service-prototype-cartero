package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/lthiede/cartero/client"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const payloadLength = 3800
const warmupDuration = 10 * time.Second
const experimentDuration = 10 * time.Second

type stringSlice []string

var logAddressFlag stringSlice
var nFlag = flag.Int("n", 1, "number of concurrent clients")
var pFlag = flag.Int("p", 1, "number of partitions")
var sFlag = flag.String("s", "localhost:8080", "server address")

func (n *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*n))
}

func (n *stringSlice) Set(value string) error {
	*n = append(*n, value)
	return nil
}

// baseline
// 16 clients sent 2073675 messages on 8 channels in 10 seconds; message rate 207367 msg/s; bw 787996500 B/s
// with ack per batch and not actually going through the partition
// 3 clients sent 2252298 messages on 3 channels in 10 seconds; message rate 225229 msg/s; bw 855873240 B/s
func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Parse()
	partitionNames := make([]string, 0, *pFlag)
	for i := range *pFlag {
		partitionNames = append(partitionNames, fmt.Sprintf("partition%d", i))
	}
	returnChans := make([]chan uint64, 0, *nFlag)
	for range *nFlag {
		returnChans = append(returnChans, make(chan uint64))
	}
	for i := range *nFlag {
		go experiment(partitionNames[i%*pFlag], returnChans[i])
	}
	var messagesSentTotal uint64
	for _, r := range returnChans {
		messagesSent, ok := <-r
		if !ok {
			fmt.Println("One client wasn't successful")
			return
		}
		messagesSentTotal += messagesSent
	}
	fmt.Printf("%d clients sent %d messages on %d channels in %d seconds; message rate %d msg/s; bw %d B/s\n",
		*nFlag,
		messagesSentTotal,
		*pFlag,
		uint64(experimentDuration.Seconds()),
		messagesSentTotal/uint64(experimentDuration.Seconds()),
		messagesSentTotal*payloadLength/uint64(experimentDuration.Seconds()))
}

func experiment(partitionName string, messagesSent chan<- uint64) {
	payload := make([]byte, payloadLength)
	rand.Read(payload)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	num, err := rand.Int(rand.Reader, big.NewInt(2000))
	if err != nil {
		fmt.Printf("Error getting random client number: %v", err)
		return
	}
	config.OutputPaths = []string{fmt.Sprintf("./client_logs_%d", num.Int64())}
	logger, err := config.Build()
	if err != nil {
		fmt.Printf("Error building logger: %v\n", err)
		return
	}
	client, err := client.New(*sFlag, "localhost:9000", "minioadmin", "minioadmin", logger)
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
		return
	}
	defer client.Close()
	err = client.CreatePartition(partitionName)
	if err != nil {
		fmt.Printf("Error creating partition: %v\n", err)
	}
	producer, err := client.NewProducer(partitionName, false)
	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		return
	}
	defer producer.Close()
	log.Println("Starting warmup")
	warmupFinished := timer(warmupDuration)
	var count uint64
warmup:
	for {
		select {
		case <-warmupFinished:
			break warmup
		default:
			err := producer.AddBatch(payload)
			if err != nil {
				return
			}
			count++
		}
	}
	startNumMessages := producer.NumMessagesAck()
	log.Printf("Starting measurements with %d messages ack\n", startNumMessages)
	experimentFinished := timer(experimentDuration)
experiment:
	for {
		select {
		case <-experimentFinished:
			break experiment
		default:
			err := producer.AddBatch(payload)
			if err != nil {
				return
			}
			count++
		}
	}
	endNumMessages := producer.NumMessagesAck()
	log.Printf("Finished measurements with %d messages ack\n", endNumMessages)
	log.Printf("Sent %d messages\n", count)
	messagesSent <- endNumMessages - startNumMessages
}

func timer(duration time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(done)
	}()
	return done
}
