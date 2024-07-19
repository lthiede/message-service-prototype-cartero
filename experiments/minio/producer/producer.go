package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/lthiede/cartero/client"
	"go.uber.org/zap"
)

type intSlice []int

var nFlag intSlice
var lFlag = flag.String("l", "", "local address")
var message = []byte{250, 216, 174, 1, 144, 191, 56, 89, 77, 254, 172, 9, 154, 235, 89, 92, 180, 27, 71, 219, 28, 127, 88, 55, 189, 141, 36, 93, 218, 64, 93, 209, 251, 206, 24, 103, 57, 28, 229, 139, 216, 139, 228, 204, 117, 184, 215, 65, 178, 152, 11, 2, 186, 99, 240, 112, 68, 89, 71, 101, 138, 132, 93, 145, 16, 35, 209, 200, 189, 117, 12, 77, 59, 221, 94, 114, 61, 131, 0, 72, 55, 173, 90, 160, 89, 29, 31, 222, 82, 67, 94, 146, 217, 128, 243, 168, 31, 87, 86, 167, 32, 165, 8, 174, 32, 145, 117, 95, 203, 185, 241, 0, 61, 124, 65, 90, 50, 35, 246, 192, 139, 242, 0, 22, 50, 86, 203, 52, 219, 140, 27, 5, 24, 7, 243, 222, 107, 255, 35, 193, 144, 91, 15, 4, 18, 158, 167, 3, 188, 70, 9, 96, 79, 187, 137, 29, 172, 123, 170, 210, 81, 82, 166, 38, 153, 80, 12, 239, 159, 130, 104, 128, 153, 5, 85, 140, 128, 2, 142, 210, 3, 170, 73, 100, 251, 230, 183, 56, 77, 84, 69, 218, 5, 219, 253, 172, 6, 137, 224, 3, 223, 75, 58, 174, 101, 46, 151, 23, 21, 233, 57, 140, 7, 173, 2, 221, 130, 177, 44, 21, 22, 197, 87, 67, 207, 9, 227, 55, 211, 36, 117, 245, 112, 144, 83, 202, 164, 149, 91, 116, 170, 210, 150, 137, 250, 251, 98, 224, 3, 135, 132, 23, 150, 217, 2, 52, 219, 0}

func (n *intSlice) String() string {
	return fmt.Sprintf("%v", []int(*n))
}

func (n *intSlice) Set(value string) error {
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("failed to parse array command line argument: %v", err)
	}
	*n = append(*n, intValue)
	return nil
}

const partitionPrefix = "testpartition"
const WarmUpPeriod = 20 * time.Second
const MeasuringPeriod = 300 * time.Second
const CoolDownPeriod = 20 * time.Second

type produceExperimentResult struct {
	numProducers int
	p25          time.Duration
	p50          time.Duration
	p75          time.Duration
	p90          time.Duration
	p99          time.Duration
	p99_9        time.Duration
	p99_99       time.Duration
	p99_999      time.Duration
	mps          float64
	errorRate    float64
}

type producerResult struct {
	latencySamples []time.Duration
	messages       int
	errors         int
}

var csvHeader = []string{"numProducers", "p25", "p50", "p75", "p90", "p99", "p99.9", "p99.99", "p99.999", "mps", "errorRate", "messageSize"}

func main() {
	flag.Var(&nFlag, "n", "number client")
	flag.Parse()
	file, err := os.Create("results_producer_minio")
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer file.Close()
	csvWriter := csv.NewWriter(file)
	csvWriter.Write(csvHeader)
	for _, i := range nFlag {
		// continue here
		e := runExperiment(i)
		log.Println(e)
		row := make([]string, 0, 12)
		row = append(row, strconv.Itoa(e.numProducers))
		row = append(row, strconv.FormatInt(e.p25.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p50.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p75.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p90.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_9.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_99.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_999.Microseconds(), 10))
		row = append(row, strconv.FormatFloat(e.mps, 'g', -1, 64))
		row = append(row, strconv.FormatFloat(e.errorRate, 'g', -1, 64))
		row = append(row, strconv.Itoa(len(message)))
		csvWriter.Write(row)
		csvWriter.Flush()
	}
}

func runExperiment(n int) produceExperimentResult {
	warmUp := make(chan int)
	go func() {
		time.Sleep(WarmUpPeriod)
		close(warmUp)
	}()
	measuring := make(chan int)
	go func() {
		time.Sleep(WarmUpPeriod + MeasuringPeriod)
		close(measuring)
	}()
	coolDown := make(chan int)
	go func() {
		time.Sleep(WarmUpPeriod + MeasuringPeriod + CoolDownPeriod)
		close(coolDown)
	}()
	flag.Parse()
	fmt.Printf("Starting %d producers \n", n)
	fmt.Printf("Using local address %s \n", *lFlag)
	resultsChan := make(chan producerResult)
	for i := 0; i < n; i++ {
		fmt.Println("Starting client")
		go runClient(i, fmt.Sprintf("%s%d", partitionPrefix, n), warmUp, measuring, coolDown, resultsChan)
	}
	fmt.Println("Wait for results")
	latencySamples := make([]time.Duration, 0)
	messages := 0
	errors := 0
	for i := 0; i < n; i++ {
		result := <-resultsChan
		latencySamples = append(latencySamples, result.latencySamples...)
		messages += result.messages
		errors += result.errors
	}
	fmt.Println("Got all results")
	slices.Sort(latencySamples)
	length := float64(len(latencySamples))
	p25Index := int(math.Round(length*0.25) - 1)
	p50Index := int(math.Round(length/2) - 1)
	p75Index := int(math.Round(length*0.75) - 1)
	p90Index := int(math.Round(length*0.9) - 1)
	p99Index := int(math.Round(length*0.99) - 1)
	p99_9Index := int(math.Round(length*0.999) - 1)
	p99_99Index := int(math.Round(length*0.9999) - 1)
	p99_999Index := int(math.Round(length*0.99999) - 1)
	p25 := latencySamples[p25Index]
	p50 := latencySamples[p50Index]
	p75 := latencySamples[p75Index]
	p90 := latencySamples[p90Index]
	p99 := latencySamples[p99Index]
	p99_9 := latencySamples[p99_9Index]
	p99_99 := latencySamples[p99_99Index]
	p99_999 := latencySamples[p99_999Index]
	mps := float64(messages) / MeasuringPeriod.Seconds()
	errorRate := float64(errors) / float64(messages)
	return produceExperimentResult{
		numProducers: n,
		p25:          p25,
		p50:          p50,
		p75:          p75,
		p90:          p90,
		p99:          p99,
		p99_9:        p99_9,
		p99_99:       p99_99,
		p99_999:      p99_999,
		mps:          mps,
		errorRate:    errorRate,
	}
}

func runClient(clientNumber int, partitionName string, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan producerResult) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v \n", err)
		os.Exit(1)
	}
	logger.Info("Created logger", zap.Int("clientNumber", clientNumber))
	c, err := client.NewWithOptions("172.18.94.70:8080", "172.18.94.80:9000", *lFlag, logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	logger.Info("Created client", zap.Int("clientNumber", clientNumber))
	client.MaxMessagesPerBatch = 1
	err = c.CreatePartition(partitionName)
	if err != nil {
		logger.Fatal("Failed to create partition", zap.Error(err))
	}
	logger.Info("Created partition", zap.Int("clientNumber", clientNumber))
	producer, err := c.NewProducer(partitionName, true)
	if err != nil {
		logger.Fatal("Failed to create ping pong client", zap.Error(err))
	}
	latencySamples := make([]time.Duration, 0)
	messages := 0
	errors := 0
	if clientNumber == 0 {
		logger.Info("Start warmup")
	}
warmUp:
	for {
		select {
		case <-warmUp:
			break warmUp
		default:
			producer.Input <- message
			select {
			case <-producer.Acks:
			case err := <-producer.Error:
				logger.Error("Failed to send request", zap.Error(err.Err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Start measuring")
	}
measuring:
	for {
		select {
		case <-measuring:
			break measuring
		default:
			if rand.Intn(100) == 0 {
				if clientNumber == 0 {
					logger.Info("Sending timed request")
				}
				start := time.Now()
				producer.Input <- message
				select {
				case <-producer.Acks:
				case err := <-producer.Error:
					logger.Error("Failed to send request", zap.Error(err.Err))
					errors++
				}
				latency := time.Since(start)
				latencySamples = append(latencySamples, latency)
			} else {
				producer.Input <- message
				select {
				case <-producer.Acks:
				case err := <-producer.Error:
					logger.Error("Failed to send request", zap.Error(err.Err))
					errors++
				}
			}
			messages++
		}
	}
	if clientNumber == 0 {
		logger.Info("Start cooldown")
	}
coolDown:
	for {
		select {
		case <-coolDown:
			break coolDown
		default:
			producer.Input <- message
			select {
			case <-producer.Acks:
			case err := <-producer.Error:
				logger.Error("Failed to send request", zap.Error(err.Err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Returning results")
	}
	resultsChan <- producerResult{
		latencySamples: latencySamples,
		messages:       messages,
		errors:         errors,
	}
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
