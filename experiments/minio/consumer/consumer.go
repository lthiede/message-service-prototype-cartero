package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/lthiede/cartero/client"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type intSlice []int

var nFlag intSlice
var cFlag intSlice
var bFlag = flag.String("b", "testpartition", "bucket name")
var oFlag = flag.String("o", "localhost:9000", "address of object storage (s3/minio)")
var aFlag = flag.String("a", "minioadmin", "access key for s3")
var sFlag = flag.String("s", "minioadmin", "secret access key for s3")

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

const WarmUpPeriod = 10 * time.Second
const MeasuringPeriod = 10 * time.Hour
const CoolDownPeriod = 10 * time.Second

// const WarmUpPeriod = 20 * time.Hour
// const MeasuringPeriod = 60 * time.Hour
// const CoolDownPeriod = 20 * time.Hour

type consumeExperimentResult struct {
	bpsDownloaded float64
	fpsDownloaded float64
	bpsConsumed   float64
	fpsConsumed   float64
	p25           time.Duration
	p50           time.Duration
	p75           time.Duration
	p90           time.Duration
	p99           time.Duration
	p99_9         time.Duration
	p99_99        time.Duration
}

var csvHeader = []string{"numConsumers", "concurrency", "bpsDownloaded", "fpsDownloaded", "bpsConsumed", "fpsConsumed", "p25", "p50", "p75", "p90", "p99", "p99.9", "p99.99"}

func main() {
	flag.Var(&nFlag, "n", "number clients")
	flag.Var(&cFlag, "c", "concurrent downloads per client")
	flag.Parse()
	fileName := "consumer_minio"
	file, err := os.Create(fileName)
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer file.Close()
	csvWriter := csv.NewWriter(file)
	csvWriter.Write(csvHeader)
	for _, n := range nFlag {
		for _, c := range cFlag {
			// continue here
			e := runExperiment(n, c)
			row := make([]string, 0, len(csvHeader))
			row = append(row, strconv.Itoa(n))
			row = append(row, strconv.Itoa(c))
			row = append(row, strconv.FormatFloat(e.bpsDownloaded, 'g', -1, 64))
			row = append(row, strconv.FormatFloat(e.fpsDownloaded, 'g', -1, 64))
			row = append(row, strconv.FormatFloat(e.bpsConsumed, 'g', -1, 64))
			row = append(row, strconv.FormatFloat(e.fpsConsumed, 'g', -1, 64))
			row = append(row, strconv.Itoa(int(e.p25.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p50.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p75.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p90.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p99.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p99_9.Microseconds())))
			row = append(row, strconv.Itoa(int(e.p99_99.Microseconds())))
			csvWriter.Write(row)
			csvWriter.Flush()
		}
	}
}

func runExperiment(n int, c int) consumeExperimentResult {
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
	resultsChan := make(chan client.MinioMetrics)
	for i := 0; i < n; i++ {
		go runClient(i, c, warmUp, measuring, coolDown, resultsChan)
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	fmt.Println("Wait for results")
	latencies := make([]time.Duration, 0)
	var bytesDownloaded uint64
	filesDownloaded := 0
	var bytesConsumed uint64
	filesConsumed := 0
	for i := 0; i < n; i++ {
		result := <-resultsChan
		latencies = append(latencies, result.FirstByteLatencies...)
		bytesDownloaded += result.BytesDownloaded
		filesDownloaded += result.FilesDownloaded
		bytesConsumed += result.BytesConsumed
		filesConsumed += result.FilesConsumed
	}
	fmt.Println("Got all results")
	slices.Sort(latencies)
	length := float64(len(latencies))
	p25Index := int(math.Round(length*0.25) - 1)
	p50Index := int(math.Round(length/2) - 1)
	p75Index := int(math.Round(length*0.75) - 1)
	p90Index := int(math.Round(length*0.9) - 1)
	p99Index := int(math.Round(length*0.99) - 1)
	p99_9Index := int(math.Round(length*0.999) - 1)
	p99_99Index := int(math.Round(length*0.9999) - 1)
	p25 := latencies[p25Index]
	p50 := latencies[p50Index]
	p75 := latencies[p75Index]
	p90 := latencies[p90Index]
	p99 := latencies[p99Index]
	p99_9 := latencies[p99_9Index]
	p99_99 := latencies[p99_99Index]
	bpsDownloaded := float64(bytesDownloaded) / MeasuringPeriod.Seconds()
	fpsDownloaded := float64(filesDownloaded) / MeasuringPeriod.Seconds()
	bpsConsumed := float64(bytesConsumed) / MeasuringPeriod.Seconds()
	fpsConsumed := float64(filesConsumed) / MeasuringPeriod.Seconds()
	fmt.Println("Aggregated results")
	return consumeExperimentResult{
		bpsDownloaded: bpsDownloaded,
		fpsDownloaded: fpsDownloaded,
		bpsConsumed:   bpsConsumed,
		fpsConsumed:   fpsConsumed,
		p25:           p25,
		p50:           p50,
		p75:           p75,
		p90:           p90,
		p99:           p99,
		p99_9:         p99_9,
		p99_99:        p99_99,
	}
}

func runClient(clientNumber int, concurrency int, warmUp chan int, measuring chan int, coolDown chan int, resultsChan chan client.MinioMetrics) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v \n", err)
		os.Exit(1)
	}
	client.Concurrency = concurrency
	consumer, err := client.NewBenchmarkConsumer(*bFlag, *oFlag, *aFlag, *sFlag, logger)
	if err != nil {
		logger.Fatal("Failed to create ping pong client", zap.Error(err))
	}
	if clientNumber == 0 {

		memStats := &runtime.MemStats{}
		runtime.ReadMemStats(memStats)
		logger.Info("Start warmup", zap.String("heapAlloc", message.NewPrinter(language.English).Sprintf("%d", memStats.HeapAlloc)))
	}
	consumer.CollectMetricsLock.Lock()
	consumer.CollectMetrics = false
	consumer.CollectMetricsLock.Unlock()
warmUp:
	for {
		select {
		case <-warmUp:
			break warmUp
		default:
			err := consumer.NextObject()
			if err == client.ErrTimeout {
				logger.Warn("Non fatal timeout", zap.Error(err))
			} else if err != nil {
				logger.Panic("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Start measuring")
	}
	consumer.CollectMetricsLock.Lock()
	consumer.CollectMetrics = true
	consumer.CollectMetricsLock.Unlock()
measuring:
	for {
		select {
		case <-measuring:
			break measuring
		default:
			err := consumer.NextObject()
			if err == client.ErrTimeout {
				logger.Warn("Non fatal timeout", zap.Error(err))
			} else if err != nil {
				logger.Panic("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Start cooldown")
	}
	consumer.CollectMetricsLock.Lock()
	consumer.CollectMetrics = false
	consumer.CollectMetricsLock.Unlock()
coolDown:
	for {
		select {
		case <-coolDown:
			break coolDown
		default:
			err := consumer.NextObject()
			if err == client.ErrTimeout {
				logger.Warn("Non fatal timeout", zap.Error(err))
			} else if err != nil {
				logger.Panic("Error while consuming", zap.Error(err))
			}
		}
	}
	if clientNumber == 0 {
		logger.Info("Returning results")
	}
	consumer.Close()
	resultsChan <- consumer.Metrics()
	if clientNumber == 0 {
		logger.Info("Finished")
	}
}
