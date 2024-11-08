package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"time"

	logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"
)

const messageLength = 3800
const warmupDuration = 60 * time.Second
const experimentDuration = 120 * time.Second

type stringSlice []string

var logAddressFlag stringSlice
var nFlag = flag.Int("n", 1, "number of concurrent clients")

func (n *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*n))
}

func (n *stringSlice) Set(value string) error {
	*n = append(*n, value)
	return nil
}

// c06 to c08
// 1 clients sent 2621459 messages in 15 seconds; message rate 174763 msg/s; bw 664102946 B/s
// 2 clients sent 3331075 messages in 15 seconds; message rate 222071 msg/s; bw 843872333 B/s
// 3 clients sent 3342376 messages in 15 seconds; message rate 222825 msg/s; bw 846735253 B/s
// 4 clients sent 3214399 messages in 15 seconds; message rate 214293 msg/s; bw 814314413 B/s

// longer experiments starting here
func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Parse()
	returnChans := make([]chan uint64, 0, *nFlag)
	for range *nFlag {
		returnChans = append(returnChans, make(chan uint64))
	}
	for _, r := range returnChans {
		go experiment(r)
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
	fmt.Printf("%d clients sent %d messages in %d seconds; message rate %d msg/s; bw %d B/s\n",
		*nFlag,
		messagesSentTotal,
		uint64(experimentDuration.Seconds()),
		messagesSentTotal/uint64(experimentDuration.Seconds()),
		messagesSentTotal*messageLength/uint64(experimentDuration.Seconds()))
}

func experiment(messagesSent chan<- uint64) {
	payload := make([]byte, 137*messageLength)
	endOffsets := make([]uint32, 137)
	rand.Read(payload)
	for i := 0; i < 137; i++ {
		endOffsets[i] = uint32((i + 1) * messageLength)
	}
	logClient, err := logclient.New(logAddressFlag,
		logclient.MaxOutstanding,
		logclient.UringEntries,
		logclient.UringFlagNoSingleIssuer)
	if err != nil {
		log.Printf("error creating log client: %v\n", err)
		close(messagesSent)
		return
	}
	err = logClient.Connect()
	if err != nil {
		log.Printf("error connecting to log nodes: %v", err)
		close(messagesSent)
		return
	}
	log.Println("Starting warmup")
	warmupFinished := timer(warmupDuration)
warmup:
	for {
		select {
		case <-warmupFinished:
			break warmup
		default:
			produce(payload, endOffsets, logClient)
		}
	}
	startLsn, err := logClient.PollCompletion()
	if err != nil {
		log.Printf("error polling highest committed lsn after warmup: %v", err)
		close(messagesSent)
		return
	}
	log.Println("Starting measurements")
	experimentFinished := timer(experimentDuration)
experiment:
	for {
		select {
		case <-experimentFinished:
			break experiment
		default:
			produce(payload, endOffsets, logClient)
		}
	}
	endLsn, err := logClient.PollCompletion()
	if err != nil {
		log.Printf("error polling highest committed lsn after experiment: %v", err)
		close(messagesSent)
		return
	}
	messagesSent <- endLsn - startLsn
}

func produce(payload []byte, endOffsets []uint32, client *logclient.ClientWrapper) error {
	_, err := client.AppendAsync(payload, endOffsets)
	if err != nil {
		_, err := client.PollCompletion()
		if err != nil {
			return fmt.Errorf("failed to poll highest committed lsn: %v", err)
		}
	}
	return nil
}

func timer(duration time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(done)
	}()
	return done
}
