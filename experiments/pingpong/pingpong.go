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

var nFlag = flag.Int("n", 31, "number of concurrent clients")
var lFlag = flag.String("l", "", "local address")
var sFlag = flag.Bool("s", false, "use streaming requests")
var mFlag = flag.Bool("m", false, "use server side synchronization")
var dFlag = flag.Bool("d", true, "use no delay")
var cFlag = flag.String("c", "proto", "codec for serializing messages")

const WarmUpPeriod = 60 * time.Second
const MeasuringPeriod = 60 * time.Second
const CoolDownPeriod = 60 * time.Second

type result struct {
	latencySamples []time.Duration
	requests       int
}

func main() {
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
	fmt.Printf("Starting %d clients \n", *nFlag)
	if *sFlag {
		fmt.Println("The clients are streaming")
	} else {
		fmt.Println("The clients send unary requests")
	}
	if *mFlag {
		fmt.Println("The requests are synchronized")
	} else {
		fmt.Println("The requests are unsynchronized")
	}
	fmt.Printf("NoDelay %t \n", *dFlag)
	fmt.Printf("Using local address %s \n", *lFlag)
	fmt.Printf("Messages are serialized with %s \n", *cFlag)
	resultsChan := make(chan result)
	for i := 0; i < *nFlag; i++ {
		go runClient(i, warmUp, measuring, coolDown, resultsChan)
	}
	fmt.Println("Wait for results")
	latencySamples := make([]time.Duration, 0)
	requests := 0
	for i := 0; i < *nFlag; i++ {
		result := <-resultsChan
		latencySamples = append(latencySamples, result.latencySamples...)
		requests += result.requests
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
	file, err := os.Create("results")
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer file.Close()
	format := `number local clients: %d
	sync: %t
	stream: %t
	no_delay: %t
	codec: %s
	length: %d,
	p25 index: %d value: %d mus,
	p50 index: %d value: %d mus,
	p75 index: %d value: %d mus,
	p90 index: %d value: %d mus,
	p99 index: %d value: %d mus,
	p99.9 index: %d value: %d mus,
	p99.99 index: %d value: %d mus,
	p99.999 index: %d value: %d mus,
	qps: %f`
	fmt.Fprintf(file, format, *nFlag, *mFlag, *sFlag, *dFlag, *cFlag, int(length),
		p25Index, p25.Microseconds(),
		p50Index, p50.Microseconds(),
		p75Index, p75.Microseconds(),
		p90Index, p90.Microseconds(),
		p99Index, p99.Microseconds(),
		p99_9Index, p99_9.Microseconds(),
		p99_99Index, p99_99.Microseconds(),
		p99_999Index, p99_999.Microseconds(),
		qps)
}

func runClient(clientNumber int, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan result) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
		os.Exit(1)
	}
	c, err := client.NewWithOptions("172.18.94.80:8080", *lFlag, *cFlag, *dFlag, logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	var pingPongClient client.PingPong
	if *sFlag {
		if *mFlag {
			pingPongClient, err = c.NewStreamPingPongSync()
		} else {
			pingPongClient, err = c.NewStreamPingPong()
		}
	} else {
		if *mFlag {
			pingPongClient, err = c.NewUnaryPingPongSync()
		} else {
			pingPongClient, err = c.NewUnaryPingPong()
		}
	}
	if err != nil {
		logger.Fatal("Failed to create ping pong client", zap.Error(err))
	}
	latencySamples := make([]time.Duration, 0)
	requests := 0
	if clientNumber == 0 {
		logger.Info("Start warmup")
	}
warmUp:
	for {
		select {
		case <-warmUp:
			break warmUp
		default:
			err := pingPongClient.PingPong()
			if err != nil {
				logger.Fatal("Failed to send request", zap.Error(err))
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
				latency, err := pingPongClient.TimedPingPong()
				if err != nil {
					logger.Fatal("Failed to send timed request", zap.Error(err))
				}
				latencySamples = append(latencySamples, latency)
			} else {
				err := pingPongClient.PingPong()
				if err != nil {
					logger.Fatal("Failed to send request", zap.Error(err))
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
			err := pingPongClient.PingPong()
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
	}
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
