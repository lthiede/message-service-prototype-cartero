package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
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
var pFlag = flag.Bool("p", true, "actually parse messages")

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
const MeasuringPeriod = 60 * time.Second
const CoolDownPeriod = 20 * time.Second

type consumeExperimentResult struct {
	numConsumers int
	p25          time.Duration
	p50          time.Duration
	p75          time.Duration
	p90          time.Duration
	p99          time.Duration
	p99_9        time.Duration
	p99_99       time.Duration
	p99_999      time.Duration
	fps          float64
	mps          float64
}

var csvHeader = []string{"numConsumers", "p25", "p50", "p75", "p90", "p99", "p99.9", "p99.99", "p99.999", "mps", "fps"}

func main() {
	flag.Var(&nFlag, "n", "number client")
	flag.Parse()
	file, err := os.Create("results_consumer_minio")
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
		row := make([]string, 0, 11)
		row = append(row, strconv.Itoa(e.numConsumers))
		row = append(row, strconv.FormatInt(e.p25.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p50.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p75.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p90.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_9.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_99.Microseconds(), 10))
		row = append(row, strconv.FormatInt(e.p99_999.Microseconds(), 10))
		row = append(row, strconv.FormatFloat(e.mps, 'g', -1, 64))
		row = append(row, strconv.FormatFloat(e.fps, 'g', -1, 64))
		csvWriter.Write(row)
		csvWriter.Flush()
	}
}

func runExperiment(n int) consumeExperimentResult {
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
	fmt.Printf("Starting %d consumers \n", n)
	fmt.Printf("Using local address %s \n", *lFlag)
	fmt.Printf("Parsing messages %t \n", *pFlag)
	resultsChan := make(chan client.MinioMetrics)
	for i := 0; i < n; i++ {
		go runClient(i, fmt.Sprintf("%s%d", partitionPrefix, 4), warmUp, measuring, coolDown, resultsChan)
	}
	fmt.Println("Wait for results")
	latencySamples := make([]time.Duration, 0)
	messages := 0
	files := 0
	for i := 0; i < n; i++ {
		result := <-resultsChan
		latencySamples = append(latencySamples, result.Latencies...)
		messages += result.Messages
		files += result.Files
	}
	fmt.Println("Got all results")
	log.Println(latencySamples)
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
	fps := float64(files) / MeasuringPeriod.Seconds()
	return consumeExperimentResult{
		numConsumers: n,
		p25:          p25,
		p50:          p50,
		p75:          p75,
		p90:          p90,
		p99:          p99,
		p99_9:        p99_9,
		p99_99:       p99_99,
		p99_999:      p99_999,
		mps:          mps,
		fps:          fps,
	}
}

func runClient(clientNumber int, partitionName string, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan client.MinioMetrics) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v \n", err)
		os.Exit(1)
	}
	c, err := client.NewWithOptions("172.18.94.70:8080", "172.18.94.80:9000", *lFlag, logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	client.MaxMessagesPerBatch = 1
	err = c.CreatePartition(partitionName)
	if err != nil {
		logger.Fatal("Failed to create partition", zap.Error(err))
	}
	consumer, err := c.NewConsumer(partitionName, 0)
	if err != nil {
		logger.Fatal("Failed to create ping pong client", zap.Error(err))
	}
	if clientNumber == 0 {
		logger.Info("Start warmup")
	}
	consumer.CollectMetrics = false
warmUp:
	for {
		select {
		case <-warmUp:
			break warmUp
		default:
			var err error
			if *pFlag {
				_, err = consumer.Consume()
			} else {
				err = consumer.ConsumeWholeObject()
			}
			if err == client.ErrTimeout {
				logger.Warn("Producer didn't produce enough messages", zap.Error(err))
			}
			if err != nil {
				logger.Warn("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Start measuring")
	}
	consumer.CollectMetrics = true
measuring:
	for {
		select {
		case <-measuring:
			break measuring
		default:
			if *pFlag {
				_, err = consumer.Consume()
			} else {
				err = consumer.ConsumeWholeObject()
			}
			if err == client.ErrTimeout {
				logger.Warn("Producer didn't produce enough messages", zap.Error(err))
			}
			if err != nil {
				logger.Warn("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Start cooldown")
	}
	consumer.CollectMetrics = false
coolDown:
	for {
		select {
		case <-coolDown:
			break coolDown
		default:
			if *pFlag {
				_, err = consumer.Consume()
			} else {
				err = consumer.ConsumeWholeObject()
			}
			if err == client.ErrTimeout {
				logger.Warn("Producer didn't produce enough messages", zap.Error(err))
			}
			if err != nil {
				logger.Warn("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Returning results")
	}
	if !*pFlag {
		consumer.Metrics.Messages = 1
	}
	resultsChan <- consumer.Metrics
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
