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
var bFlag = flag.Bool("b", false, "create the server")

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
// with ack per batch, not actually through partition and payload outside of proto
// 3 clients sent 3084007 messages on 3 channels in 10 seconds; message rate 308400 msg/s; bw 1171922660 B/s
// with ack of partial batches, not actually through partition and payload outside of proto
// 3 clients sent 2973996 messages on 3 channels in 10 seconds; message rate 297399 msg/s; bw 1130118480 B/s
// with ack of partial batches, through partition and payload outside of proto
// 3 clients sent 1052167 messages on 3 channels in 10 seconds; message rate 105216 msg/s; bw 399823460 B/s
// with ack per batch, through partition and payload outside of proto
// 3 clients sent 1293554 messages on 3 channels in 10 seconds; message rate 129355 msg/s; bw 491550520 B/s
// ack per batch, through partition, separate payload, no ack go routine
// 3 clients sent 1608517 messages on 3 channels in 10 seconds; message rate 160851 msg/s; bw 611236460 B/s

// longer experiments starting here
// ack per batch, through partition, separate payload, lightweight ack go routine, bug in acks
// 3 clients sent 22005625 messages on 3 channels in 120 seconds; message rate 183380 msg/s; bw 696844791 B/s
// ack per batch, through partition, separate payload, lightweight ack go routine, bug in acks fixed
// 3 clients sent 7458691 messages on 3 channels in 60 seconds; message rate 124311 msg/s; bw 472383763 B/s
// ack per batch, through partition, separate payload, no ack go routine, bug in acks fixed
// 3 clients sent 14786821 messages on 3 channels in 120 seconds; message rate 123223 msg/s; bw 468249331 B/s
// ack per batch, through partition, separate payload, ack go routine, no actual log interactions
// 3 clients sent 36990274 messages on 3 channels in 120 seconds; message rate 308252 msg/s; bw 1171358676 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per batch
// 3 clients sent 19495648 messages on 3 channels in 120 seconds; message rate 162463 msg/s; bw 617362186 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per batch, log build for release
// 3 clients sent 24492586 messages on 3 channels in 120 seconds; message rate 204104 msg/s; bw 775598556 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per batch, ack routine spinning on atomic for lsn
// 3 clients sent 18157706 messages on 3 channels in 120 seconds; message rate 151314 msg/s; bw 574994023 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per buffer of batches collected over 75 ms (75ms seems best)
// 3 clients sent 19275626 messages on 3 channels in 120 seconds; message rate 160630 msg/s; bw 610394823 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per buffer of batches collected over 75 ms, log build for release
// 3 clients sent 19205208 messages on 3 channels in 120 seconds; message rate 160043 msg/s; bw 608164920 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per batch, log build for release, acknowledge only on every 4th lsn
// 3 clients sent 24205160 messages on 3 channels in 120 seconds; message rate 201709 msg/s; bw 766496733 B/s
// ack per batch, through partition, separate payload, ack go routine, calling rust once per batch, log build for release, avoid select in connection response handler
// 3 clients sent 24054049 messages on 3 channels in 120 seconds; message rate 200450 msg/s; bw 761711551 B/s
func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Parse()
	// if *bFlag {
	// 	logger, err := zap.NewDevelopment()
	// 	if err != nil {
	// 		fmt.Printf("Failed to create logger: %v", err)
	// 		return
	// 	}
	// 	server, err := server.New([]string{}, *sFlag, *&logAddressFlag, logger)
	// 	if err != nil {
	// 		fmt.Printf("Failed to create server: %v", err)
	// 		return
	// 	}
	// 	defer server.Close()
	// }
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
