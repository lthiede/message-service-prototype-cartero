package main

import (
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

/*
Results:
8 client server with 125 clients each
dm-c01
length: 28787,
	p50 index: 14393 value: 1359 mus,
	p90 index: 25907 value: 3020 mus,
	p95 index: 27347 value: 5199 mus,
	p99 index: 28498 value: 12257 mus
dm-c02
length: 30822,
	p50 index: 15410 value: 1345 mus,
	p90 index: 27739 value: 2999 mus,
	p95 index: 29280 value: 5026 mus,
	p99 index: 30513 value: 12698 mus
dm-c03
length: 31503,
	p50 index: 15751 value: 1322 mus,
	p90 index: 28352 value: 2955 mus,
	p95 index: 29927 value: 5164 mus,
	p99 index: 31187 value: 13103 mus
dm-c04
length: 30991,
	p50 index: 15495 value: 1346 mus,
	p90 index: 27891 value: 2994 mus,
	p95 index: 29440 value: 4981 mus,
	p99 index: 30680 value: 12545 mus
dm-c05
length: 31023,
	p50 index: 15511 value: 1341 mus,
	p90 index: 27920 value: 2925 mus,
	p95 index: 29471 value: 5057 mus,
	p99 index: 30712 value: 12495 mus
dm-c06
length: 31209,
	p50 index: 15604 value: 1356 mus,
	p90 index: 28087 value: 3235 mus,
	p95 index: 29648 value: 5156 mus,
	p99 index: 30896 value: 11783 mus
dm-c07
length: 29772,
	p50 index: 14885 value: 1369 mus,
	p90 index: 26794 value: 4229 mus,
	p95 index: 28282 value: 5975 mus,
	p99 index: 29473 value: 12150 mus
dm-c08
length: 33539,
	p50 index: 16769 value: 1245 mus,
	p90 index: 30184 value: 1917 mus,
	p95 index: 31861 value: 3866 mus,
	p99 index: 33203 value: 11933
1 client server with 125 client
dm-c01
length: 102804,
	p50 index: 51401 value: 274 mus,
	p90 index: 92523 value: 868 mus,
	p95 index: 97663 value: 1430 mus,
	p99 index: 101775 value: 2549 mus
*/

const NumberClients = 125
const WarmUpPeriod = 60 * time.Second
const MeasuringPeriod = 60 * time.Second
const CoolDownPeriod = 60 * time.Second

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
	resultsChan := make(chan []time.Duration)
	for i := 0; i < NumberClients; i++ {
		go runClients(i, warmUp, measuring, coolDown, resultsChan)
	}
	fmt.Println("Wait for results")
	results := make([]time.Duration, 0)
	for i := 0; i < NumberClients; i++ {
		result := <-resultsChan
		results = append(results, result...)
	}
	fmt.Println("Got all results")
	start := time.Now()
	slices.Sort(results)
	end := time.Now()
	fmt.Printf("Sorted results took %d s", int(end.Sub(start).Seconds()))
	length := float64(len(results))
	p50Index := int(math.Round(length/2) - 1)
	p90Index := int(math.Round(length*0.9) - 1)
	p95Index := int(math.Round(length*0.95) - 1)
	p99Index := int(math.Round(length*0.99) - 1)
	p50 := results[p50Index]
	p90 := results[p90Index]
	p95 := results[p95Index]
	p99 := results[p99Index]
	file, err := os.Create("results")
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer file.Close()
	format := `length: %d,
	p50 index: %d value: %d mus,
	p90 index: %d value: %d mus,
	p95 index: %d value: %d mus,
	p99 index: %d value: %d mus`
	fmt.Fprintf(file, format, int(length), p50Index, p50.Microseconds(), p90Index, p90.Microseconds(), p95Index, p95.Microseconds(), p99Index, p99.Microseconds())
}

func runClients(clientNumber int, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan []time.Duration) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
		os.Exit(1)
	}
	client, err := client.New("c09.lab.dm.informatik.tu-darmstadt.de:8080", logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	results := make([]time.Duration, 0)
	if clientNumber == 0 {
		logger.Info("Start warmup")
	}
warmUp:
	for {
		select {
		case <-warmUp:
			break warmUp
		default:
			err := client.PingPong()
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
				result, err := client.TimedPingPong()
				if err != nil {
					logger.Fatal("Failed to send timed request", zap.Error(err))
				}
				results = append(results, result)
			} else {
				err := client.PingPong()
				if err != nil {
					logger.Fatal("Failed to send request", zap.Error(err))
				}
			}
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
			err := client.PingPong()
			if err != nil {
				logger.Fatal("Failed to send request", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Returning results")
	}
	resultsChan <- results
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
