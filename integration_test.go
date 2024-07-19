package cartero_test

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/lthiede/cartero/client"
	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func setupServer(partitionNames []string) (*server.Server, error) {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	server, err := server.New(partitionNames, "localhost:8080", "localhost:9000", logger)
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
	client, err := client.New("localhost:8080", "localhost:9000", logger)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func waitForAcks(expectedNumAck int, producer *client.Producer, timeout <-chan int) error {
	for int(producer.NumMessagesAck()) < expectedNumAck {
		time.Sleep(100 * time.Millisecond)
		select {
		case err := <-producer.Error:
			return err.Err
		case <-timeout:
			return errors.New("Waiting for acks timed out")
		default:
			continue
		}
	}
	return nil
}

func produce(numberMessages int, partitionName string, client *client.Client, maxInFlight int, wg *sync.WaitGroup, errChan chan<- error) {
	producer, err := client.NewProducer(partitionName, false)
	if err != nil {
		errChan <- err
		wg.Done()
		log.Println("Failed to create producer")
		return
	}
	defer producer.Close()
	timeout := make(chan int)
	go func() {
		time.Sleep(20 * time.Second)
		close(timeout)
	}()
	for numberProduced := 0; numberProduced < numberMessages; {
		currentMessage := []byte(fmt.Sprintf("%s_%d", partitionName, numberProduced))
		select {
		case producer.Input <- currentMessage:
			numberProduced++
		case <-timeout:
			errChan <- errors.New("Produce timed out")
			wg.Done()
			log.Println("Produce timed out")
			return
		}
		err := waitForAcks(numberProduced-maxInFlight, producer, timeout)
		if err != nil {
			errChan <- err
			wg.Done()
			return
		}
	}
	err = waitForAcks(numberMessages, producer, timeout)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	wg.Done()
	log.Println("Exiting naturally")
}

func waitForSafeOffset(startOffset uint64, expectedEndOfSafeOffsets uint64, inOrder bool, partitionName string, consumer *client.Consumer, timeout <-chan int) error {
	for currentOffset := startOffset; currentOffset < expectedEndOfSafeOffsets; currentOffset++ {
		message, err := consumer.Consume()
		if err != nil {
			return fmt.Errorf("error consuming: %v", err)
		}
		if inOrder {
			expectedMessage := fmt.Sprintf("%s_%d", partitionName, currentOffset)
			if string(message) != expectedMessage {
				return fmt.Errorf("Client received message out of order expected %s but got %s", expectedMessage, string(message))
			}
		}
		select {
		case <-timeout:
			return errors.New("waiting for safe consume offset timed out")
		default:
			continue
		}
	}
	return nil
}

func consume(numberMessages int, startOffset uint64, inOrder bool, partitionName string, client *client.Client, wg *sync.WaitGroup, errChan chan<- error) {
	consumer, err := client.NewConsumer(partitionName, uint64(startOffset))
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	defer consumer.Close()
	timeout := make(chan int)
	go func() {
		time.Sleep(20 * time.Second)
		close(timeout)
	}()
	err = waitForSafeOffset(startOffset, startOffset+uint64(numberMessages), inOrder, partitionName, consumer, timeout)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	wg.Done()
}

// TestBasicHappyCase tests a simple setup with two partitions with one consumer and one producer each
func TestBasicHappyCase(t *testing.T) {
	server, err := setupServer([]string{"partition0", "partition1"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	producerClient, err := setupClient()
	if err != nil {
		t.Fatal(err)
	}
	defer producerClient.Close()
	consumerClient, err := setupClient()
	if err != nil {
		t.Fatal(err)
	}
	defer consumerClient.Close()
	var wg sync.WaitGroup
	wg.Add(4)
	errChan := make(chan error, 4)
	go produce(100, "partition0", producerClient, 20, &wg, errChan)
	go produce(100, "partition1", producerClient, 20, &wg, errChan)
	go consume(100, 0, true, "partition0", consumerClient, &wg, errChan)
	go consume(100, 0, true, "partition1", consumerClient, &wg, errChan)
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

// TestPubSub tests two producers producing on the same partition with two consumers each consuming all messages
func TestPubSub(t *testing.T) {
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
	go produce(100, "partition0", client0, 50, &wg, errChan)
	go produce(100, "partition0", client1, 50, &wg, errChan)
	go consume(200, 0, false, "partition0", client0, &wg, errChan)
	go consume(200, 0, false, "partition0", client1, &wg, errChan)
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

// TestMaxPublishDelay tests publishing due to reaching the max publish delay before reaching the max number of messages in a batch
func TestMaxPublishDelay(t *testing.T) {
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
	wg.Add(2)
	errChan := make(chan error, 2)
	go produce(30, "partition0", client1, 5, &wg, errChan)
	time.Sleep(10 * time.Second)
	go consume(30, 0, true, "partition0", client0, &wg, errChan)
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

// TestReadingFromStartOffset tests reading starting from a start offset > 0
func TestReadingFromStartOffset(t *testing.T) {
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
	wg.Add(2)
	errChan := make(chan error, 2)
	go produce(30, "partition0", client1, 7, &wg, errChan)
	go consume(15, 15, true, "partition0", client0, &wg, errChan)
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

// TestAddingPartitions tests adding partitions to the running server
func TestAddingPartitions(t *testing.T) {
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
	err = client0.CreatePartition("testpartition")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	errChan := make(chan error, 2)
	go produce(100, "testpartition", client0, 20, &wg, errChan)
	go consume(100, 0, true, "testpartition", client1, &wg, errChan)
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

// TestAddingPartitionSimultaneously tests adding the partition at the same time from multiple clients
func TestAddingPartitionSimultaneously(t *testing.T) {
	server, err := setupServer([]string{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	numClients := 5
	results := make(chan error)
	for range numClients {
		go func() {
			client0, err := setupClient()
			if err != nil {
				results <- err
			}
			defer client0.Close()
			err = client0.CreatePartition("testpartition")
			results <- err
		}()
	}
	for range numClients {
		err := <-results
		if err != nil {
			log.Fatal(err)
		}
	}
}
