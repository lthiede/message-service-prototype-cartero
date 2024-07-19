package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"slices"
	"time"

	"github.com/lthiede/cartero/client"
	"go.uber.org/zap"
)

var lFlag = flag.String("l", "", "local address")

var nFlag = flag.Int("n", 0, "number clients")

const partitionName = "testpartition"

const WarmUpPeriod = 20 * time.Second
const MeasuringPeriod = 60 * time.Second
const CoolDownPeriod = 20 * time.Second

type result struct {
	latencySamples []time.Duration
	requests       int
	errors         int
}

func main() {
	flag.Parse()
	if *nFlag != 0 {
		runExperiment(*nFlag)
	} else {
		for _, i := range []int{24, 26, 28, 30, 32, 34, 36, 38, 40} {
			runExperiment(i)
		}
	}
}

func runExperiment(n int) {
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
	fmt.Printf("Starting %d clients \n", n)
	fmt.Printf("Using local address %s \n", *lFlag)
	resultsChan := make(chan result)
	for i := 0; i < n; i++ {
		go runClient(i, warmUp, measuring, coolDown, resultsChan)
	}
	fmt.Println("Wait for results")
	latencySamples := make([]time.Duration, 0)
	requests := 0
	errors := 0
	for i := 0; i < n; i++ {
		result := <-resultsChan
		latencySamples = append(latencySamples, result.latencySamples...)
		requests += result.requests
		errors += result.errors
	}
	fmt.Println("Got all results")
	start := time.Now()
	slices.Sort(latencySamples)
	end := time.Now()
	fmt.Printf("Sorted results took %d s", int(end.Sub(start).Seconds()))
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
	qps := float64(requests) / MeasuringPeriod.Seconds()
	errorRate := float64(errors) / float64(requests)
	file, err := os.Create(fmt.Sprintf("results_ping_pong_16_cores_7x%d", n))
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer file.Close()
	format := `number local clients: %d
	length: %d,
	p25 index: %d value: %d mus,
	p50 index: %d value: %d mus,
	p75 index: %d value: %d mus,
	p90 index: %d value: %d mus,
	p99 index: %d value: %d mus,
	p99.9 index: %d value: %d mus,
	p99.99 index: %d value: %d mus,
	p99.999 index: %d value: %d mus,
	qps: %f
	error rate: %f`
	fmt.Fprintf(file, format, n, int(length),
		p25Index, p25.Microseconds(),
		p50Index, p50.Microseconds(),
		p75Index, p75.Microseconds(),
		p90Index, p90.Microseconds(),
		p99Index, p99.Microseconds(),
		p99_9Index, p99_9.Microseconds(),
		p99_99Index, p99_99.Microseconds(),
		p99_999Index, p99_999.Microseconds(),
		qps, errorRate)
}

func runClient(clientNumber int, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan result) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v \n", err)
		os.Exit(1)
	}
	c, err := client.NewWithOptions("172.18.94.80:8080", "172.18.94.80:9000", *lFlag, logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	err = c.CreatePartition(partitionName)
	if err != nil {
		logger.Fatal("Failed to create partition", zap.Error(err))
	}
	pingPongClient, err := c.NewPingPong(partitionName)
	if err != nil {
		logger.Fatal("Failed to create ping pong client", zap.Error(err))
	}
	latencySamples := make([]time.Duration, 0)
	requests := 0
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
			err := pingPongClient.SendPingPong()
			if err != nil {
				logger.Error("Failed to send request", zap.Error(err))
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
				latency, err := pingPongClient.SendTimedPingPong()
				if err != nil {
					logger.Error("Failed to send timed request", zap.Error(err))
					errors++
				}
				latencySamples = append(latencySamples, latency)
			} else {
				err := pingPongClient.SendPingPong()
				if err != nil {
					logger.Error("Failed to send request", zap.Error(err))
					errors++
				}
			}
			requests++
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
			err := pingPongClient.SendPingPong()
			if err != nil {
				logger.Fatal("Failed to send request", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Returning results")
	}
	resultsChan <- result{
		latencySamples: latencySamples,
		requests:       requests,
		errors:         errors,
	}
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
