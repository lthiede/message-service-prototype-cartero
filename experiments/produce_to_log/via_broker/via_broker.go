package main

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"os"
	"slices"
	"time"

	"github.com/lthiede/cartero/client"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const measurementPeriod = 10 * time.Second
const basePartitionName = "partition"
const maxOutstandingAsync uint32 = 3
const numPossiblePayloads = 1000

var possiblePayloads [][]byte = make([][]byte, numPossiblePayloads)
var experimentDuration time.Duration

type stringSlice []string

var logAddressFlag stringSlice // o
var pFlag = flag.Int("p", 8, "number of partitions")
var nFlag = flag.Int("n", 8, "network connections to broker")
var mFlag = flag.Int("m", 1000, "messages per second")
var lFlag = flag.Int("l", 3800, "bytes per message")
var aFlag = flag.Int("a", 0, "max number of bytes per batch")
var cFlag = flag.Int("c", 1, "max pending batches per producer")
var eFlag = flag.Int("e", 60, "experiment and warmup duration")
var sFlag = flag.String("s", "localhost:8080", "server address")
var bFlag = flag.Bool("b", false, "create the server")

func (n *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*n))
}

func (n *stringSlice) Set(value string) error {
	*n = append(*n, value)
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
	MaxPending                    int
	Partitions                    int
	Connections                   int
	MessageSize                   int
	MaxBatchSize                  int
	MessagesPerSecondConfigured   int
}

func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
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
	for i := range numPossiblePayloads {
		payload := make([]byte, *lFlag)
		n, err := cryptorand.Read(payload)
		if n != *lFlag {
			logger.Error("Failed to create possible message",
				zap.Int("bytesPerMessage", *lFlag),
				zap.Error(err),
			)
			return
		}
		possiblePayloads[i] = payload
	}
	result, err := oneRun(logger)
	if err != nil {
		logger.Error("Run failed",
			zap.Int("numPartitions", *pFlag),
			zap.Int("numConnections", *nFlag),
			zap.Int("bytesPerMessage", *lFlag),
			zap.Int("maxBytesPerBatch", *aFlag),
			zap.Int("messagesPerSecond", *mFlag),
			zap.Error(err))
		return
	}
	text, err := json.Marshal(result)
	if err != nil {
		logger.Error("Marshal of results failed",
			zap.Int("numPartitions", *pFlag),
			zap.Int("numConnections", *nFlag),
			zap.Int("bytesPerMessage", *lFlag),
			zap.Int("maxBytesPerBatch", *aFlag),
			zap.Int("messagesPerSecond", *mFlag),
			zap.Error(err))
		return
	}
	fmt.Println(string(text))
	output, err := os.Create(fmt.Sprintf("results_%d_p_%d_c_%d_b_%d_b_%d_mxs.json", *pFlag, *nFlag, *lFlag, *aFlag, *mFlag))
	if err != nil {
		logger.Error("Creating output file failed",
			zap.Int("numPartitions", *pFlag),
			zap.Int("numConnections", *nFlag),
			zap.Int("bytesPerMessage", *lFlag),
			zap.Int("maxBytesPerBatch", *aFlag),
			zap.Int("messagesPerSecond", *mFlag),
			zap.Error(err))
		return
	}
	_, err = output.Write(text)
	if err != nil {
		logger.Error("Writing to output file failed",
			zap.Int("numPartitions", *pFlag),
			zap.Int("numConnections", *nFlag),
			zap.Int("bytesPerMessage", *lFlag),
			zap.Int("maxBytesPerBatch", *aFlag),
			zap.Int("messagesPerSecond", *mFlag),
			zap.Error(err))
		return
	}
}

func oneRun(logger *zap.Logger) (*Result, error) {
	setupClient, err := createClient(fmt.Sprintf("./setup_client_%d_%d_%d_%d_%d", *pFlag, *nFlag, *lFlag, *aFlag, *mFlag))
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err)
	}
	defer setupClient.Close()
	oneRunTopicName := fmt.Sprintf("%s_%d_%d_%d_%d_%d_", basePartitionName, *pFlag, *nFlag, *lFlag, *aFlag, *mFlag)
	err = setupClient.CreatePartition(oneRunTopicName, uint32(*pFlag))
	if err != nil {
		return nil, fmt.Errorf("error creating partitions: %v", err)
	}
	partitionNames := make([]string, *pFlag)
	for i := range *pFlag {
		partitionNames[i] = fmt.Sprintf("%s%d", oneRunTopicName, i)
	}
	var numProducers int
	if *nFlag > *pFlag {
		numProducers = *nFlag
	} else {
		numProducers = *pFlag
	}
	returnChans := make([]chan clientResult, numProducers)
	for i := range numProducers {
		returnChans[i] = make(chan clientResult)
	}
	clients := make([]*client.Client, *nFlag)
	for i := range *nFlag {
		c, err := createClient(fmt.Sprintf("client_%d_%d_%d_%d_%d_%d", i, *pFlag, *nFlag, *lFlag, *aFlag, *mFlag))
		if err != nil {
			return nil, fmt.Errorf("error creating client: %v", err)
		}
		clients[i] = c
	}
	for i := range numProducers {
		partitionName := partitionNames[i%*pFlag]
		c := clients[i%*cFlag]
		go oneClient(partitionName, float64(*mFlag)/float64(numProducers), c, logger, returnChans[i])
	}
	numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
	aggregatedMessagesPerSecond := make([]float64, numMeasurements)
	aggregatedBytesPerSecond := make([]float64, numMeasurements)
	latencies := make([]time.Duration, 0)
	for i, r := range returnChans {
		clientResult, ok := <-r
		if !ok {
			return nil, errors.New("one client wasn't successful")
		}
		logger.Info("Received results", zap.Int("clientNum", i))
		if len(clientResult.MessagesPerSecondMeasurements) != numMeasurements*2 {
			return nil, fmt.Errorf("client returned %d measurements but expected %d", len(clientResult.MessagesPerSecondMeasurements), numMeasurements*2)
		}
		for i := range numMeasurements {
			aggregatedMessagesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[numMeasurements+i]
			aggregatedBytesPerSecond[i] += clientResult.MessagesPerSecondMeasurements[numMeasurements+i] * float64(*lFlag)
		}
		latencies = append(latencies, clientResult.LatencyMeasurements...)
	}
	for i, c := range clients {
		logger.Info("Closing client", zap.Int("clientNum", i))
		c.Close()
	}
	err = setupClient.DeletePartition(oneRunTopicName, uint32(*pFlag))
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
		MaxPending:                    *cFlag,
		Partitions:                    *pFlag,
		Connections:                   *nFlag,
		MessageSize:                   *lFlag,
		MaxBatchSize:                  *aFlag,
		MessagesPerSecondConfigured:   *mFlag,
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

func oneClient(partitionName string, messages float64, c *client.Client, logger *zap.Logger, messagesSent chan<- clientResult) {
	producer, err := c.NewProducer(partitionName, uint32(*cFlag))
	if err != nil {
		logger.Error("Error creating producer", zap.Error(err))
		close(messagesSent)
		return
	}
	producer.MaxBatchSize = uint32(*aFlag)
	producer.MaxPublishDelay = 0 // turns off publishing on a timer
	logger.Info("Starting experiment")
	experimentScheduler, quitExperiment := timer(time.Duration(2*int64(experimentDuration))+time.Second, messages)
	measurements := measure(producer)
experiment:
	for {
		select {
		case <-experimentScheduler:
			err := producer.AddMessage(possiblePayloads[mathrand.IntN(numPossiblePayloads)])
			if err != nil {
				logger.Error("Error adding message", zap.Error(err))
			}
		case <-quitExperiment:
			logger.Info("Finished experiment")
			break experiment
		case producerErr := <-producer.AsyncError:
			err := producerErr.Err
			logger.Error("Async error in producer", zap.Error(err))
		}
	}
	r, ok := <-measurements
	if !ok {
		logger.Error("Measure go routine had error")
		close(messagesSent)
		return
	}
	logger.Info("Received measurements", zap.String("partitionName", partitionName))
	producer.Close()
	messagesSent <- r
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

func measure(producer *client.Producer) chan clientResult {
	messagesSent := make(chan clientResult)
	go func() {
		numMeasurements := int(experimentDuration.Seconds() / measurementPeriod.Seconds())
		messagesPerSecondMeasurements := make([]float64, 0, 2*numMeasurements)
		startNumMessages := producer.NumMessagesSent()
		for range numMeasurements {
			start := time.Now()
			time.Sleep(measurementPeriod)
			endNumMessages := producer.NumMessagesSent()
			duration := time.Since(start)
			messagesPerSecondMeasurements = append(messagesPerSecondMeasurements, float64(endNumMessages-startNumMessages)/duration.Seconds())
			startNumMessages = endNumMessages
		}
		producer.StartMeasuringLatencies()
		for range numMeasurements {
			start := time.Now()
			time.Sleep(measurementPeriod)
			endNumMessages := producer.NumMessagesSent()
			duration := time.Since(start)
			messagesPerSecondMeasurements = append(messagesPerSecondMeasurements, float64(endNumMessages-startNumMessages)/duration.Seconds())
			startNumMessages = endNumMessages
		}
		latencies := producer.StopMeasuringLatencies()
		messagesSent <- clientResult{
			MessagesPerSecondMeasurements: messagesPerSecondMeasurements,
			LatencyMeasurements:           latencies,
		}
	}()
	return messagesSent
}
