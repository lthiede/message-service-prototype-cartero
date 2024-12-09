package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/lthiede/cartero/client"
	"github.com/lthiede/cartero/server"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const payloadLength = 3800
const warmupDuration = 60 * time.Second
const experimentDuration = 60 * time.Second
const measurementPeriod = 10 * time.Second
const basePartitionName = "partition"

type stringSlice []string
type intSlice []int

var logAddressFlag stringSlice
var partitionsFlag intSlice
var producersPerPartitionFlag intSlice
var messagesPerSecondFlag intSlice
var sFlag = flag.String("s", "localhost:8080", "server address")
var bFlag = flag.Bool("b", false, "create the server")

func (n *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*n))
}

func (n *stringSlice) Set(value string) error {
	*n = append(*n, value)
	return nil
}

func (n *intSlice) String() string {
	return fmt.Sprintf("%v", []int(*n))
}

func (n *intSlice) Set(value string) error {
	i, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	*n = append(*n, i)
	return nil
}

type Result struct {
	MessagesPerSecondMeasurements []uint64
	BytesPerSecondMeasurements    []uint64
	NumLatencyMeasurements        int
	Latency50Pct                  float64
	Latency75Pct                  float64
	Latency90Pct                  float64
	Latency99Pct                  float64
	Latency999Pct                 float64
	Latency9999Pct                float64
	Partitions                    int
	ClientsPerPartitions          int
	MessagesTarget                int
}

func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Var(&partitionsFlag, "p", "number of partitions")
	flag.Var(&messagesPerSecondFlag, "m", "target messages per second")
	flag.Var(&producersPerPartitionFlag, "n", "number of producers per partition")
	flag.Parse()
	if *bFlag {
		logger, err := zap.NewDevelopment()
		if err != nil {
			fmt.Printf("Failed to create logger: %v", err)
			return
		}
		server, err := server.New([]string{}, *sFlag, logAddressFlag, logger)
		if err != nil {
			fmt.Printf("Failed to create server: %v", err)
			return
		}
		defer server.Close()
	}
	results := map[int]map[int]map[int]Result{}
	for _, messages := range messagesPerSecondFlag {
		results[messages] = map[int]map[int]Result{}
		for _, partitions := range partitionsFlag {
			results[messages][partitions] = map[int]Result{}
			for _, clientsPerPartition := range producersPerPartitionFlag {
				result, err := oneRun(partitions, clientsPerPartition, messages)
				if err != nil {
					fmt.Printf("Run with %d partitions and %d clients per partitions failed: %v", partitions, clientsPerPartition, err)
					return
				}
				results[messages][partitions][clientsPerPartition] = *result
				text, err := json.Marshal(result)
				if err != nil {
					fmt.Printf("Marshal of results with %d partitions and %d clients per partitions failed: %v", partitions, clientsPerPartition, err)
					return
				}
				fmt.Println(string(text))
			}
		}
	}
	text, err := json.Marshal(results)
	if err != nil {
		fmt.Printf("Marshal of results failed: %v", err)
		return
	}
	output, err := os.Create("results.json")
	if err != nil {
		fmt.Printf("Failed to create output file: %v", err)
		return
	}
	_, err = output.Write(text)
	if err != nil {
		fmt.Printf("Failed to write results to file: %v", err)
	}
}

func oneRun(partitions int, clientsPerPartition int, messages int) (*Result, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{fmt.Sprintf("./setup_client_%d_%d", partitions, clientsPerPartition)}
	logger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("error building logger: %v", err)
	}
	setupClient, err := client.New(*sFlag, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err)
	}
	defer setupClient.Close()
	err = setupClient.CreatePartition(basePartitionName, uint32(partitions))
	if err != nil {
		return nil, fmt.Errorf("error creating partitions: %v", err)
	}
	partitionNames := make([]string, partitions)
	for i := range partitions {
		partitionNames[i] = fmt.Sprintf("%s%d", basePartitionName, i)
	}
	numClients := clientsPerPartition * partitions
	returnChans := make([]chan clientResult, numClients)
	for i := range numClients {
		returnChans[i] = make(chan clientResult)
	}
	for i := range numClients {
		partitionName := partitionNames[i%partitions]
		go oneClient(partitionName, fmt.Sprintf("client_%d_%d_%s_%d", partitions, clientsPerPartition, partitionName, i), float64(messages)/float64(numClients), returnChans[i])
	}
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	aggregatedMessagesPerSecond := make([]uint64, numMeasurements)
	aggregatedBytesPerSecond := make([]uint64, numMeasurements)
	latencies := make([]time.Duration, 0)
	for _, r := range returnChans {
		clientResult, ok := <-r
		if !ok {
			return nil, errors.New("one client wasn't successful")
		}
		fmt.Println("Received client result")
		if len(clientResult.MessagesPerSecondMeasurements) != numMeasurements {
			return nil, fmt.Errorf("client returned %d measurements but expected %d", len(clientResult.MessagesPerSecondMeasurements), numMeasurements)
		}
		for i := range numMeasurements {
			aggregatedMessagesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[i]
			aggregatedBytesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[i] * payloadLength
		}
		latencies = append(latencies, clientResult.LatencyMeasurements...)
	}
	err = setupClient.DeletePartition(basePartitionName, uint32(partitions))
	if err != nil {
		return nil, fmt.Errorf("error deleting partitions: %v", err)
	}
	slices.Sort(latencies)
	return &Result{
		MessagesPerSecondMeasurements: aggregatedMessagesPerSecond,
		BytesPerSecondMeasurements:    aggregatedBytesPerSecond,
		Partitions:                    partitions,
		NumLatencyMeasurements:        len(latencies),
		Latency50Pct:                  pct(latencies, 0.5),
		Latency75Pct:                  pct(latencies, 0.75),
		Latency90Pct:                  pct(latencies, 0.9),
		Latency99Pct:                  pct(latencies, 0.99),
		Latency999Pct:                 pct(latencies, 0.999),
		Latency9999Pct:                pct(latencies, 0.9999),
		ClientsPerPartitions:          clientsPerPartition,
		MessagesTarget:                messages,
	}, nil
}

func pct(latencies []time.Duration, pct float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	ordinal := int(math.Ceil(float64(len(latencies)) * pct))
	ordinalZeroIndexed := ordinal - 1
	return latencies[ordinalZeroIndexed].Seconds()
}

type clientResult struct {
	MessagesPerSecondMeasurements []uint64
	LatencyMeasurements           []time.Duration
}

func oneClient(partitionName string, logName string, messages float64, messagesSent chan<- clientResult) {
	payload := make([]byte, payloadLength)
	rand.Read(payload)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{logName}
	logger, err := config.Build()
	if err != nil {
		fmt.Printf("Error building logger: %v\n", err)
		close(messagesSent)
		return
	}
	client, err := client.New(*sFlag, logger)
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
		close(messagesSent)
		return
	}
	defer client.Close()
	producer, err := client.NewProducer(partitionName, false)
	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		close(messagesSent)
		return
	}
	defer producer.Close()
	waitBetweenMessages := time.Duration(float64(time.Second) / float64(messages))
	log.Println("Starting warmup")
	warmupFinished := timer(warmupDuration)
warmup:
	for {
		select {
		case <-warmupFinished:
			break warmup
		default:
			time.Sleep(waitBetweenMessages)
			err := producer.AddMessage(payload)
			if err != nil {
				fmt.Printf("Error adding message: %v\n", err)
				close(messagesSent)
				return
			}
		}
	}
	startNumMessages := producer.NumMessagesAck()
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	log.Printf("Starting experiment with %d measurement periods of %f seconds\n", numMeasurements, measurementPeriod.Seconds())
	messagesPerSecondMeasurements := make([]uint64, numMeasurements)
	producer.StartMeasuringLatencies()
	for i := range numMeasurements {
		log.Printf("iteration %d\n", i)
		periodFinished := timer(measurementPeriod)
		start := time.Now()
	experiment:
		for {
			select {
			case <-periodFinished:
				break experiment
			default:
				time.Sleep(waitBetweenMessages)
				err := producer.AddMessage(payload)
				if err != nil {
					fmt.Printf("Error adding message: %v\n", err)
					close(messagesSent)
					return
				}
			}
		}
		endNumMessages := producer.NumMessagesAck()
		duration := time.Since(start)
		messagesPerSecondMeasurements[i] = (endNumMessages - startNumMessages) / uint64(duration.Seconds())
		startNumMessages = endNumMessages
	}
	latencies := producer.StopMeasuringLatencies()
	select {
	case err := <-producer.Error:
		fmt.Printf("Producer had asynchronous error: %v", err.Err)
		close(messagesSent)
		return
	default:
	}
	messagesSent <- clientResult{
		MessagesPerSecondMeasurements: messagesPerSecondMeasurements,
		LatencyMeasurements:           latencies,
	}
}

func timer(duration time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(done)
	}()
	return done
}
