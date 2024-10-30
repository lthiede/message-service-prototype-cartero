package cartero_test

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lthiede/cartero/client"
	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type maxLSN struct {
	lsns  []uint64
	index atomic.Uint32
	m     sync.RWMutex
}

func setupServer(partitionNames []string) (*server.Server, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	server, err := server.New(partitionNames, "localhost:8080", []string{"127.0.0.1:50000"}, logger)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func setupClient() (*client.Client, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	client, err := client.New("localhost:8080", "localhost:9000", "minioadmin", "minioadmin", logger)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func waitForAcks(expectedNumAck int, producer *client.Producer) error {
	for int(producer.NumMessagesAck()) < expectedNumAck {
		time.Sleep(time.Millisecond)
		select {
		case err := <-producer.Error:
			return err.Err
		default:
			continue
		}
	}
	return nil
}

func produce(numberMessages int, partitionName string, client *client.Client, maxInFlight int, outputLSN *maxLSN, wg *sync.WaitGroup, errChan chan<- error) {
	producer, err := client.NewProducer(partitionName, false)
	if err != nil {
		errChan <- err
		wg.Done()
		log.Println("Failed to create producer")
		return
	}
	defer producer.Close()
	for numberProduced := 0; numberProduced < numberMessages; {
		currentMessage := []byte(fmt.Sprintf("%s_%d", partitionName, numberProduced))
		producer.Input <- currentMessage
		numberProduced++
		err := waitForAcks(numberProduced-maxInFlight, producer)
		if err != nil {
			errChan <- err
			wg.Done()
			fmt.Printf("Err waiting for acks 1: %v\n", err)
			return
		}
	}
	err = waitForAcks(numberMessages, producer)
	if err != nil {
		errChan <- err
		wg.Done()
		fmt.Printf("Err waiting for acks 2: %v\n", err)
		return
	}
	log.Println("finished waiting for acks")
	lastLSN := producer.LastLSNPlus1() - 1
	index := outputLSN.index.Add(1) - 1
	outputLSN.lsns[index] = lastLSN
	if index == uint32(len(outputLSN.lsns)-1) {
		outputLSN.m.Unlock()
	}
	wg.Done()
	log.Println("Produce exiting naturally")
}

func waitForLSN(expectedLSN uint64, consumer *client.Consumer) error {
	for consumer.EndOfSafeLSNsExclusively() <= expectedLSN {
		time.Sleep(10 * time.Second)
	}
	return nil
}

func consume(partitionName string, client *client.Client, inputLsn *maxLSN, wg *sync.WaitGroup, errChan chan<- error) {
	consumer, err := client.NewConsumer(partitionName, 0)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	defer consumer.Close()
	inputLsn.m.RLock()
	defer inputLsn.m.RUnlock()
	expectedLSN := slices.Max(inputLsn.lsns)
	err = waitForLSN(expectedLSN, consumer)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	wg.Done()
	log.Println("Consume exiting naturally")
}

// TestRustSegmentStore tests the integrating of the broker with the distributed log
// the distributed log is currently restricted because it allows only writes
func TestRustSegmentStore(t *testing.T) {
	server, err := setupServer([]string{"partition0", "partition1"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client0, err := setupClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client0.Close()
	client1, err := setupClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()
	var wg sync.WaitGroup
	wg.Add(4)
	errChan := make(chan error, 4)
	outputLSN := &maxLSN{
		lsns: make([]uint64, 2),
	}
	outputLSN.m.Lock()
	go produce(10000, "partition0", client0, 50, outputLSN, &wg, errChan)
	go produce(10000, "partition1", client1, 50, outputLSN, &wg, errChan)
	go consume("partition1", client0, outputLSN, &wg, errChan)
	go consume("partition0", client1, outputLSN, &wg, errChan)
	wg.Wait()
	for {
		select {
		case err := <-errChan:
			t.Error(err)
		default:
			return
		}
	}
}
