package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"slices"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var nFlag = flag.Int("n", 31, "number of concurrent clients")
var lFlag = flag.String("l", "", "local address")
var aFlag = flag.String("a", "", "address")
var pFlag = flag.Bool("p", false, "use protobuf")

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
	fmt.Printf("Using local address %s \n", *lFlag)
	fmt.Printf("Using protobug %t \n", *pFlag)
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
	proto encoding: %t
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
	fmt.Fprintf(file, format, *nFlag, *pFlag, int(length),
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
	dialer := net.Dialer{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP(*lFlag),
			Port: 0,
		},
	}
	conn, err := dialer.Dial("tcp", *aFlag)
	if err != nil {
		logger.Fatal("Failed to dial server", zap.Error(err), zap.String("localAddr", *lFlag), zap.String("serverAddr", *aFlag))
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
			if *pFlag {
				protoPingPong(conn, logger)
			} else {
				pingPong(conn, logger)
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
				if *pFlag {
					start := time.Now()
					protoPingPong(conn, logger)
					end := time.Now()
					latencySamples = append(latencySamples, end.Sub(start))
				} else {
					start := time.Now()
					pingPong(conn, logger)
					end := time.Now()
					latencySamples = append(latencySamples, end.Sub(start))
				}
			} else {
				if *pFlag {
					protoPingPong(conn, logger)
				} else {
					pingPong(conn, logger)
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
			if *pFlag {
				protoPingPong(conn, logger)
			} else {
				pingPong(conn, logger)
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

func pingPong(conn net.Conn, logger *zap.Logger) {
	ping := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	_, err := conn.Write(ping)
	if err != nil {
		logger.Fatal("Failed to send request", zap.Error(err))
	}
	n, err := conn.Read(ping)
	if err != nil || n != 14 {
		logger.Fatal("Failed to read response")
	}
}

func protoPingPong(conn net.Conn, logger *zap.Logger) {
	ping := pb.Ping{}
	pingBytes, err := proto.Marshal(&ping)
	if err != nil {
		logger.Fatal("Failed to marshal ping", zap.Error(err))
	}
	pingWireMessage := make([]byte, 0, 4+len(pingBytes))
	pingWireMessage = binary.BigEndian.AppendUint32(pingWireMessage, uint32(len(pingBytes)))
	pingWireMessage = append(pingWireMessage, pingBytes...)
	_, err = conn.Write(pingWireMessage)
	if err != nil {
		logger.Fatal("Failed to send request", zap.Error(err))
	}
	resLengthBytes := make([]byte, 4)
	n, err := conn.Read(resLengthBytes)
	if err != nil {
		logger.Fatal("Failed to read response length", zap.Error(err))
	}
	if n != 4 {
		logger.Fatal("Not enough bytes encoding response length", zap.Int("n", n))
	}
	var resLength uint32
	err = binary.Read(bytes.NewReader(resLengthBytes), binary.BigEndian, &resLength)
	if err != nil {
		logger.Fatal("Failed to encode response length", zap.Error(err))
	}
	pongBytes := make([]byte, resLength)
	n, err = conn.Read(pongBytes)
	if err != nil {
		logger.Fatal("Failed to read response", zap.Error(err))
	}
	if n != int(resLength) {
		logger.Fatal("Response is too short", zap.Int("n", n))
	}
	pong := pb.Pong{}
	err = proto.Unmarshal(pongBytes, &pong)
	if err != nil {
		logger.Fatal("Failed to unmarshal pong", zap.Error(err))
	}
}
