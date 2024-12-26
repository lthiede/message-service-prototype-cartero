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
const maxOutstandingAsync uint32 = 524288

type stringSlice []string
type intSlice []int

var logAddressFlag stringSlice     // o
var partitionsFlag intSlice        // p
var connectionsFlag intSlice       // n
var messagesPerSecondFlag intSlice // m
var messageSizes intSlice          // l
var batchSizes intSlice            // a
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
	MessagesPerSecondMeasurements []float64
	BytesPerSecondMeasurements    []float64
	NumLatencyMeasurements        int
	Latency50Pct                  float64
	Latency75Pct                  float64
	Latency90Pct                  float64
	Latency99Pct                  float64
	Latency999Pct                 float64
	Latency9999Pct                float64
	Sync                          bool
	Partitions                    int
	Connections                   int
	MessageSize                   int
	MaxBatchSize                  int
	MessagesPerSecondConfigured   int
}

func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Var(&partitionsFlag, "p", "number of partitions")
	flag.Var(&messagesPerSecondFlag, "m", "target messages per second")
	flag.Var(&connectionsFlag, "n", "number of connections to broker")
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
		for _, messages := range messagesPerSecondFlag {
			for _, partitions := range partitionsFlag {
				for _, batchSize := range batchSizes {
					for _, connections := range connectionsFlag {
						result, err := oneRun(partitions, connections, messageSize, batchSize, messages, logger)
						if err != nil {
							logger.Error("Run failed",
								zap.Int("numPartitions", partitions),
								zap.Int("numConnections", connections),
								zap.Int("bytesPerMessage", messageSize),
								zap.Int("maxBytesPerBatch", batchSize),
								zap.Int("messagesPerSecond", messages),
								zap.Error(err))
							return
						}
						text, err := json.Marshal(result)
						if err != nil {
							logger.Error("Marshal of results failed",
								zap.Int("numPartitions", partitions),
								zap.Int("numConnections", connections),
								zap.Int("bytesPerMessage", messageSize),
								zap.Int("maxBytesPerBatch", batchSize),
								zap.Int("messagesPerSecond", messages),
								zap.Error(err))
							return
						}
						fmt.Println(string(text))
						output, err := os.Create(fmt.Sprintf("results_%d_p_%d_c_%d_b_%d_b_%d_mxs.json", partitions, connections, messageSize, batchSize, messages))
						if err != nil {
							logger.Error("Creating output file failed",
								zap.Int("numPartitions", partitions),
								zap.Int("numConnections", connections),
								zap.Int("bytesPerMessage", messageSize),
								zap.Int("maxBytesPerBatch", batchSize),
								zap.Int("messagesPerSecond", messages),
								zap.Error(err))
							return
						}
						_, err = output.Write(text)
						if err != nil {
							logger.Error("Writing to output file failed",
								zap.Int("numPartitions", partitions),
								zap.Int("numConnections", connections),
								zap.Int("bytesPerMessage", messageSize),
								zap.Int("maxBytesPerBatch", batchSize),
								zap.Int("messagesPerSecond", messages),
								zap.Error(err))
							return
						}
					}
				}
			}
		}
	}
}

func oneRun(partitions int, connections int, messageSize int, maxBatchSize int, messages int, logger *zap.Logger) (*Result, error) {
	setupClient, err := createClient(fmt.Sprintf("./setup_client_%d_%d_%d_%d_%d", partitions, connections, messageSize, maxBatchSize, messages))
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err)
	}
	defer setupClient.Close()
	oneRunTopicName := fmt.Sprintf("%s_%d_%d_%d_%d_%d_", basePartitionName, partitions, connections, messageSize, maxBatchSize, messages)
	err = setupClient.CreatePartition(oneRunTopicName, uint32(partitions))
	if err != nil {
		return nil, fmt.Errorf("error creating partitions: %v", err)
	}
	partitionNames := make([]string, partitions)
	for i := range partitions {
		partitionNames[i] = fmt.Sprintf("%s%d", oneRunTopicName, i)
	}
	var numProducers int
	if connections > partitions {
		numProducers = connections
	} else {
		numProducers = partitions
	}
	returnChans := make([]chan clientResult, numProducers)
	for i := range numProducers {
		returnChans[i] = make(chan clientResult)
	}
	clients := make([]*client.Client, connections)
	for i := range connections {
		c, err := createClient(fmt.Sprintf("client_%d_%d_%d_%d_%d", partitions, connections, messageSize, maxBatchSize, messages))
		if err != nil {
			return nil, fmt.Errorf("error creating client: %v", err)
		}
		clients[i] = c
	}
	for i := range numProducers {
		partitionName := partitionNames[i%partitions]
		c := clients[i%connections]
		go oneClient(partitionName, messageSize, maxBatchSize, float64(messages)/float64(numProducers), c, logger, returnChans[i])
	}
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	aggregatedMessagesPerSecond := make([]float64, numMeasurements*2)
	aggregatedBytesPerSecond := make([]float64, numMeasurements*2)
	latencies := make([]time.Duration, 0)
	for _, r := range returnChans {
		clientResult, ok := <-r
		if !ok {
			return nil, errors.New("one client wasn't successful")
		}
		logger.Info("Received client result")
		if len(clientResult.MessagesPerSecondMeasurements) != numMeasurements*2 {
			return nil, fmt.Errorf("client returned %d measurements but expected %d", len(clientResult.MessagesPerSecondMeasurements), numMeasurements*2)
		}
		for i := range numMeasurements * 2 {
			aggregatedMessagesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[i]
			aggregatedBytesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[i] * float64(messageSize)
		}
		latencies = append(latencies, clientResult.LatencyMeasurements...)
	}
	for _, c := range clients {
		c.Close()
	}
	err = setupClient.DeletePartition(oneRunTopicName, uint32(partitions))
	if err != nil {
		return nil, fmt.Errorf("error deleting partitions: %v", err)
	}
	logger.Info("Successfully deleted partition", zap.String("partitionName", oneRunTopicName))
	slices.Sort(latencies)
	logger.Info("Returning measurements", zap.Float64s("messagesPerSecond", aggregatedMessagesPerSecond))
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
		Connections:                   connections,
		MessageSize:                   messageSize,
		MaxBatchSize:                  maxBatchSize,
		MessagesPerSecondConfigured:   messages,
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
	MessagesPerSecondMeasurements []float64
	LatencyMeasurements           []time.Duration
}

func createClient(name string) (*client.Client, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	config.OutputPaths = []string{name}
	logger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %v", err)
	}
	c, err := client.New(*sFlag, logger)
	if err != nil {
		logger.Error("Error creating client", zap.Error(err))
		return nil, fmt.Errorf("failed to create client: %v", err)
	}
	return c, err
}

func oneClient(partitionName string, messageSize int, maxBatchSize int, messages float64, c *client.Client, logger *zap.Logger, messagesSent chan<- clientResult) {
	payload := make([]byte, messageSize)
	rand.Read(payload)
	producer, err := c.NewProducer(partitionName, false)
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
		producer.MaxOutstanding = maxOutstandingAsync
	}
	logger.Info("Starting experiment")
	experimentScheduler, quitExperiment := timer(time.Duration(2*int64(experimentDuration))+time.Second, messages)
	go measure(producer, logger, messagesSent)
experiment:
	for {
		select {
		case <-experimentScheduler:
			err := producer.AddMessage(payload)
			if err != nil {
				logger.Error("Error adding message", zap.Error(err))
				close(messagesSent)
				return
			}
		case <-quitExperiment:
			break experiment
		}
	}
}

func timer(duration time.Duration, messages float64) (<-chan struct{}, <-chan struct{}) {
	scheduler := make(chan struct{}, 200)
	quit := make(chan struct{})
	go func() {
		if messages == 0 {
			close(scheduler)
			time.Sleep(duration)
			close(quit)
			return
		}
		scheduler <- struct{}{}
		var numScheduled int64
		waitTime := time.Duration(float64(int64(time.Second)) / messages)
		closedQuit := false
		start := time.Now()
		for {
			passed := time.Since(start)
			if passed >= duration && !closedQuit {
				closedQuit = true
				close(quit)
			}
			shouldBeScheduled := int64(passed) / int64(waitTime)
		inner_loop:
			for numScheduled < shouldBeScheduled {
				select {
				case scheduler <- struct{}{}:
					numScheduled++
				default:
					if closedQuit {
						return
					} else {
						break inner_loop
					}
				}
			}
		}
	}()
	return scheduler, quit
}

func measure(producer *client.Producer, logger *zap.Logger, messagesSent chan<- clientResult) {
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	messagesPerSecondMeasurements := make([]float64, 0, 2*numMeasurements)
	startNumMessages := producer.NumMessagesAck()
	for range numMeasurements {
		start := time.Now()
		time.Sleep(measurementPeriod)
		endNumMessages := producer.NumMessagesAck()
		duration := time.Since(start)
		messagesPerSecondMeasurements = append(messagesPerSecondMeasurements, float64(endNumMessages-startNumMessages)/duration.Seconds())
		startNumMessages = endNumMessages
	}
	producer.StartMeasuringLatencies()
	for range numMeasurements {
		start := time.Now()
		time.Sleep(measurementPeriod)
		endNumMessages := producer.NumMessagesAck()
		duration := time.Since(start)
		messagesPerSecondMeasurements = append(messagesPerSecondMeasurements, float64(endNumMessages-startNumMessages)/duration.Seconds())
		startNumMessages = endNumMessages
	}
	select {
	case err := <-producer.AsyncError:
		logger.Error("Producer had asynchronous error", zap.Error(err.Err))
		close(messagesSent)
		return
	default:
	}
	latencies := producer.StopMeasuringLatencies()
	logger.Info("Returning latencies")
	messagesSent <- clientResult{
		MessagesPerSecondMeasurements: messagesPerSecondMeasurements,
		LatencyMeasurements:           latencies,
	}
}
