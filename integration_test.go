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
	client, err := client.New("localhost:8080", logger)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func waitForAcks(expectedNumAck int, producer *client.Producer) error {
	for int(producer.NumMessagesAck()) < expectedNumAck {
		time.Sleep(time.Millisecond)
		select {
		case err := <-producer.AsyncError:
			return err.Err
		default:
			continue
		}
	}
	return nil
}

func produce(numberMessages int, partitionName string, client *client.Client, maxInFlight int, outputLSN *maxLSN, wg *sync.WaitGroup, errChan chan<- error) {
	producer, err := client.NewProducer(partitionName, 1024)
	if err != nil {
		errChan <- err
		wg.Done()
		log.Println("Failed to create producer")
		return
	}
	defer producer.Close()
	for numberProduced := 0; numberProduced < numberMessages; {
		currentMessage := []byte(fmt.Sprintf("%s_%d", partitionName, numberProduced))
		producer.AddMessage(currentMessage)
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
	fmt.Printf("lastLSN %d\n", lastLSN)
	index := outputLSN.index.Add(1) - 1
	outputLSN.lsns[index] = lastLSN
	if index == uint32(len(outputLSN.lsns)-1) {
		fmt.Printf("unlock %s\n", partitionName)
		outputLSN.m.Unlock()
	}
	wg.Done()
	log.Println("Produce exiting naturally")
}

func waitForLSN(expectedLSN uint64, consumer *client.Consumer) error {
	for currentLSN := consumer.EndOfSafeLSNsExclusively(); currentLSN <= expectedLSN; currentLSN = consumer.EndOfSafeLSNsExclusively() {
		fmt.Printf("currentLSN %d\n", currentLSN)
		time.Sleep(10 * time.Millisecond)
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
	fmt.Printf("expectedLSN %d\n", expectedLSN)
	err = waitForLSN(expectedLSN, consumer)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	wg.Done()
	log.Println("Consume exiting naturally")
}

// TestRustSegmentStore tests parallel writes and reads on different partitions
func TestMultiplePartitions(t *testing.T) {
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
	outputLSN0 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN0.m.Lock()
	outputLSN1 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN1.m.Lock()
	go produce(10000, "partition0", client0, 50, outputLSN0, &wg, errChan)
	go produce(10000, "partition1", client1, 50, outputLSN1, &wg, errChan)
	go consume("partition1", client0, outputLSN1, &wg, errChan)
	go consume("partition0", client1, outputLSN0, &wg, errChan)
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

// TestRustSegmentStore tests parallel writes and reads on one partition
func TestMultipleProducersAndConsumers(t *testing.T) {
	server, err := setupServer([]string{"partition0"})
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
	go produce(10000, "partition0", client1, 50, outputLSN, &wg, errChan)
	go consume("partition0", client0, outputLSN, &wg, errChan)
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

// TestCreatePartition tests creating partitions and writing and reading from them
func TestCreatePartition(t *testing.T) {
	server, err := setupServer([]string{})
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
	err = client0.CreatePartition("partition", 2)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(4)
	errChan := make(chan error, 2)
	outputLSN0 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN0.m.Lock()
	outputLSN1 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN1.m.Lock()
	go produce(10000, "partition0", client0, 50, outputLSN0, &wg, errChan)
	go produce(10000, "partition1", client1, 50, outputLSN1, &wg, errChan)
	go consume("partition1", client0, outputLSN1, &wg, errChan)
	go consume("partition0", client1, outputLSN0, &wg, errChan)
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

// TestDeletePartition tests deleting and recreating a partitions
func TestDeletePartition(t *testing.T) {
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
	var wg0 sync.WaitGroup
	wg0.Add(2)
	errChan0 := make(chan error, 2)
	outputLSN0 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN0.m.Lock()
	go produce(4000, "partition0", client1, 50, outputLSN0, &wg0, errChan0)
	go consume("partition0", client0, outputLSN0, &wg0, errChan0)
	wg0.Wait()
for_loop:
	for {
		select {
		case err := <-errChan0:
			t.Error(err)
		default:
			break for_loop
		}
	}
	err = client0.DeletePartition("partition", 2)
	if err != nil {
		t.Fatal(err)
	}
	err = client0.CreatePartition("partition", 1)
	if err != nil {
		t.Fatal(err)
	}
	var wg1 sync.WaitGroup
	wg1.Add(2)
	errChan1 := make(chan error, 2)
	outputLSN1 := &maxLSN{
		lsns: make([]uint64, 1),
	}
	outputLSN1.m.Lock()
	go produce(4000, "partition0", client1, 50, outputLSN1, &wg1, errChan1)
	go consume("partition0", client0, outputLSN1, &wg1, errChan1)
	wg1.Wait()
	for {
		select {
		case err := <-errChan1:
			t.Error(err)
		default:
			return
		}
	}
}
