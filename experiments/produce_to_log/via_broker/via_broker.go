package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/lthiede/cartero/client"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const measurementPeriod = 10 * time.Second
const basePartitionName = "partition"

type stringSlice []string
type intSlice []int

var logAddressFlag stringSlice // o
var partitionsFlag intSlice    // p
// var producersPerPartitionFlag intSlice // n
// var messagesPerSecondFlag intSlice     // m
var messageSizes intSlice // l
var batchSizes intSlice   // a
var cFlag = flag.Bool("c", false, "send messages synchronously")
var eFlag = flag.Int("e", 60, "experiment and warmup duration")
var sFlag = flag.String("s", "localhost:8080", "server address")
var bFlag = flag.Bool("b", false, "create the server")

var experimentDuration time.Duration

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
	Sync                          bool
	Partitions                    int
	MessageSize                   int
	MaxBatchSize                  int
}

func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Var(&partitionsFlag, "p", "number of partitions")
	// flag.Var(&messagesPerSecondFlag, "m", "target messages per second")
	// flag.Var(&producersPerPartitionFlag, "n", "number of producers per partition")
	flag.Var(&messageSizes, "l", "number of bytes per message")
	flag.Var(&batchSizes, "a", "max number of bytes per batch")
	flag.Parse()
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{fmt.Sprintf("./global")}
	logger, err := config.Build()
	if err != nil {
		return
	}
	experimentDuration = time.Duration(*eFlag * int(time.Second))
	// if *bFlag {
	// 	logger, err := zap.NewDevelopment()
	// 	if err != nil {
	// 		fmt.Printf("Failed to create logger: %v", err)
	// 		return
	// 	}
	// 	server, err := server.New([]string{}, *sFlag, logAddressFlag, logger)
	// 	if err != nil {
	// 		fmt.Printf("Failed to create server: %v", err)
	// 		return
	// 	}
	// 	defer server.Close()
	// }
	for _, messageSize := range messageSizes {
		//	for _, messages := range messagesPerSecondFlag {
		for _, partitions := range partitionsFlag {
			// for _, clientsPerPartition := range producersPerPartitionFlag {
			for _, batchSize := range batchSizes {
				result, err := oneRun(partitions, messageSize, batchSize)
				if err != nil {
					logger.Error("Run failed",
						zap.Int("numPartitions", partitions),
						zap.Int("bytesPerMessage", messageSize),
						zap.Int("maxBytesPerBatch", batchSize),
						zap.Error(err))
					return
				}
				text, err := json.Marshal(result)
				if err != nil {
					logger.Error("Marshal of results failed",
						zap.Int("numPartitions", partitions),
						zap.Int("bytesPerMessage", messageSize),
						zap.Int("maxBytesPerBatch", batchSize),
						zap.Error(err))
					return
				}
				fmt.Println(string(text))
				output, err := os.Create(fmt.Sprintf("results_%d_p_%d_b_%d_b.json", partitions, messageSize, batchSize))
				if err != nil {
					logger.Error("Creating output file failed",
						zap.Int("numPartitions", partitions),
						zap.Int("bytesPerMessage", messageSize),
						zap.Int("maxBytesPerBatch", batchSize),
						zap.Error(err))
					return
				}
				_, err = output.Write(text)
				if err != nil {
					logger.Error("Writing to output file failed",
						zap.Int("numPartitions", partitions),
						zap.Int("bytesPerMessage", messageSize),
						zap.Int("maxBytesPerBatch", batchSize),
						zap.Error(err))
					return
				}
			}
			// }
		}
		//	}
	}
}

func oneRun(partitions int, messageSize int, maxBatchSize int) (*Result, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{fmt.Sprintf("./setup_client_%d_%d_%d", partitions, messageSize, maxBatchSize)}
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
	numClients := partitions
	returnChans := make([]chan clientResult, numClients)
	for i := range numClients {
		returnChans[i] = make(chan clientResult)
	}
	for i := range numClients {
		partitionName := partitionNames[i]
		go oneClient(partitionName, messageSize, maxBatchSize, returnChans[i])
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
			aggregatedBytesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[i] * uint64(messageSize)
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
		NumLatencyMeasurements:        len(latencies),
		Latency50Pct:                  pct(latencies, 0.5),
		Latency75Pct:                  pct(latencies, 0.75),
		Latency90Pct:                  pct(latencies, 0.9),
		Latency99Pct:                  pct(latencies, 0.99),
		Latency999Pct:                 pct(latencies, 0.999),
		Latency9999Pct:                pct(latencies, 0.9999),
		Sync:                          *cFlag,
		Partitions:                    partitions,
		MessageSize:                   messageSize,
		MaxBatchSize:                  maxBatchSize,
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

func oneClient(partitionName string, messageSize int, maxBatchSize int, messagesSent chan<- clientResult) {
	payload := make([]byte, messageSize)
	rand.Read(payload)
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{fmt.Sprintf("%s_%d_b_%d_b", partitionName, messageSize, maxBatchSize)}
	logger, err := config.Build()
	if err != nil {
		logger.Error("Error building logger", zap.Error(err))
		close(messagesSent)
		return
	}
	client, err := client.New(*sFlag, logger)
	if err != nil {
		logger.Error("Error creating client", zap.Error(err))
		close(messagesSent)
		return
	}
	defer client.Close()
	producer, err := client.NewProducer(partitionName, false)
	if err != nil {
		logger.Error("Error creating producer", zap.Error(err))
		close(messagesSent)
		return
	}
	defer producer.Close()
	producer.MaxBatchSize = uint32(maxBatchSize)
	producer.MaxPublishDelay = 0 // turns off publishing on a timer
	if *cFlag {
		producer.MaxOutstanding = 1
	} else {
		producer.MaxOutstanding = 1024
	}
	logger.Info("Starting warmup", zap.Int("duration", int(experimentDuration.Seconds())))
	warmupFinished := timer(experimentDuration)
warmup:
	for {
		select {
		case <-warmupFinished:
			break warmup
		default:
			logger.Info("Calling add message")
			err := producer.AddMessage(payload)
			if err != nil {
				logger.Error("Error adding message", zap.Error(err))
				close(messagesSent)
				return
			}
		}
	}
	startNumMessages := producer.NumMessagesAck()
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	logger.Info("Starting experiment", zap.Int("numMeasurements", numMeasurements), zap.Float64("measurementPeriod", measurementPeriod.Seconds()))
	messagesPerSecondMeasurements := make([]uint64, numMeasurements)
	producer.StartMeasuringLatencies()
	for i := range numMeasurements {
		logger.Info("iteration", zap.Int("num", i))
		periodFinished := timer(measurementPeriod)
		start := time.Now()
	experiment:
		for {
			select {
			case <-periodFinished:
				break experiment
			default:
				err := producer.AddMessage(payload)
				if err != nil {
					logger.Error("Error adding message", zap.Error(err))
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
	case err := <-producer.AsyncError:
		logger.Error("Producer had asynchronous error", zap.Error(err.Err))
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
