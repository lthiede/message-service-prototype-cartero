package cartero_test

import (
	"errors"
	"fmt"
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
	config.Level.SetLevel(zapcore.ErrorLevel)
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

func produce(numberMessages int, partitionName string, client *client.Client, lowerFrequency bool, wg *sync.WaitGroup, errChan chan<- error) {
	producer, err := client.NewProducer(partitionName)
	if err != nil {
		errChan <- err
		wg.Done()
		return
	}
	defer producer.Close()
	timeout := make(chan int)
	go func() {
		time.Sleep(20 * time.Second)
		close(timeout)
	}()
	produceErrChan := make(chan error)
	go func() {
		for numberProduced := 0; numberProduced < numberMessages; numberProduced++ {
			if lowerFrequency && numberProduced%7 == 0 {
				time.Sleep(200 * time.Millisecond)
			}
			currentMessage := []byte(fmt.Sprintf("%s_%d", partitionName, numberProduced))
			select {
			case producer.Input <- currentMessage:
			case <-timeout:
				produceErrChan <- errors.New("Produce timed out")
				return
			}
		}
		close(produceErrChan)
	}()
	var receiveAckErr error
waitingForAcks:
	for numberAck := 0; numberAck < numberMessages; {
		select {
		case <-producer.Ack:
			numberAck++
		case err := <-producer.Error:
			receiveAckErr = err.Err
			break waitingForAcks
		case <-timeout:
			receiveAckErr = errors.New("Waiting for acks timed out")
			break waitingForAcks
		}
	}
	produceErr := <-produceErrChan
	if receiveAckErr != nil && produceErr != nil {
		errChan <- fmt.Errorf("produce error %v, ack error %v", produceErr, receiveAckErr)
	} else if receiveAckErr != nil {
		errChan <- receiveAckErr
	} else if produceErr != nil {
		errChan <- produceErr
	}
	wg.Done()
}

func consume(numberMessages int, startOffset int, partitionName string, client *client.Client, checkContent bool, wg *sync.WaitGroup, errChan chan<- error) {
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
	for numberConsumed := 0; numberConsumed < numberMessages; numberConsumed++ {
		select {
		case message := <-consumer.Output:
			if !checkContent {
				continue
			}
			expectedMessage := fmt.Sprintf("%s_%d", partitionName, numberConsumed+startOffset)
			if string(message.Message) != expectedMessage {
				errChan <- fmt.Errorf("Received unexpected message %s, expected %s", string(message.Message), expectedMessage)
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
	go produce(100, "partition0", producerClient, false, &wg, errChan)
	go produce(100, "partition1", producerClient, false, &wg, errChan)
	go consume(100, 0, "partition0", consumerClient, true, &wg, errChan)
	go consume(100, 0, "partition1", consumerClient, true, &wg, errChan)
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
	go produce(100, "partition0", client0, false, &wg, errChan)
	go produce(100, "partition0", client1, false, &wg, errChan)
	go consume(200, 0, "partition0", client0, false, &wg, errChan)
	go consume(200, 0, "partition0", client1, false, &wg, errChan)
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
	errChan := make(chan error, 4)
	go produce(30, "partition0", client1, true, &wg, errChan)
	go consume(30, 0, "partition0", client0, true, &wg, errChan)
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
	errChan := make(chan error, 4)
	go produce(30, "partition0", client1, true, &wg, errChan)
	go consume(15, 15, "partition0", client0, true, &wg, errChan)
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

// TODO: test offset out of cache
// TODO: test removing producers
