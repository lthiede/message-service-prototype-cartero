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
	server, err := server.New(partitionNames, "localhost:8080", logger)
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

func produce(numberMessages int, partitionName string, client *client.Client, maxInFlight int, wg *sync.WaitGroup, errChan chan<- error) {
	producer, err := client.NewProducer(partitionName)
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
	numberAck := 0
	inFlight := 0
	for numberProduced := 0; numberProduced < numberMessages; numberProduced++ {
		currentMessage := []byte(fmt.Sprintf("%s_%d", partitionName, numberProduced))
		select {
		case producer.Input <- currentMessage:
			inFlight++
		case <-timeout:
			errChan <- errors.New("Produce timed out")
			wg.Done()
			log.Println("Produce timed out")
			return
		}
		if inFlight == maxInFlight {
			select {
			case ack := <-producer.AckOutput:
				log.Printf("batch %d acknowledged %d messages in total", ack.BatchId, int(ack.NumMessagesAck))
				numberAck = int(ack.NumMessagesAck)
			case err := <-producer.Error:
				errChan <- err.Err
				wg.Done()
				log.Println("Received error waiting for acks because max in flight requests reached")
				return
			case <-timeout:
				errChan <- errors.New("Waiting for acks timed out")
				wg.Done()
				log.Println("Received timeout waiting for acks because max in flight requests reached")
				return
			}
			inFlight = 0
		}
	}
	for numberAck < numberMessages {
		select {
		case ack := <-producer.AckOutput:
			numberAck = int(ack.NumMessagesAck)
		case err := <-producer.Error:
			errChan <- err.Err
			wg.Done()
			log.Println("Received error waiting for acks because all batches produced")
			return
		case <-timeout:
			errChan <- errors.New("Waiting for acks timed out")
			wg.Done()
			log.Println("Received timeout waiting for acks because all batches produced")
			return
		}
	}
	wg.Done()
	log.Println("Exiting naturally")
}

func consume(numberMessages int, startOffset uint64, partitionName string, client *client.Client, wg *sync.WaitGroup, errChan chan<- error) {
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
	for endOfSafeOffsets := startOffset; endOfSafeOffsets < startOffset+uint64(numberMessages); {
		select {
		case endOfSafeOffsets = <-consumer.EndOfSafeOffsetsExclusivelyOut:
			if endOfSafeOffsets < startOffset {
				errChan <- errors.New("Received offset notification for offset < startOffset")
				wg.Done()
				return
			}
		case <-timeout:
			errChan <- errors.New("Consume timed out")
			wg.Done()
			return
		}
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
	go consume(100, 0, "partition0", consumerClient, &wg, errChan)
	go consume(100, 0, "partition1", consumerClient, &wg, errChan)
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
	go consume(200, 0, "partition0", client0, &wg, errChan)
	go consume(200, 0, "partition0", client1, &wg, errChan)
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
	go consume(30, 0, "partition0", client0, &wg, errChan)
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
	go consume(15, 15, "partition0", client0, &wg, errChan)
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
	go consume(100, 0, "testpartition", client1, &wg, errChan)
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
