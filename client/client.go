package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const numberClients int = 2
const numberPartitions int = 4

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()
	var clientWg sync.WaitGroup
	clientWg.Add(numberClients)
	for i := 0; i < numberClients; i++ {
		go client(i, &clientWg, logger)
	}
	clientWg.Wait()
	logger.Info("Clients finished")
}

func client(i int, clientWg *sync.WaitGroup, logger *zap.Logger) {
	defer clientWg.Done()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial("localhost:8080", opts...)
	if err != nil {
		logger.Error("Failed to dial server", zap.Int("clientNumber", i), zap.Error(err))
		return
	}
	defer conn.Close()
	logger.Info("Dialed server", zap.Int("clientNumber", i))
	var partitionWg sync.WaitGroup
	if numberPartitions > 4 {
		logger.Error("Too many partitions", zap.Int("numberPartitions", numberPartitions))
		return
	}
	partitionWg.Add(numberPartitions)
	for j := 0; j < numberPartitions; j++ {
		go produce(i, fmt.Sprintf("partition%d", j), conn, &partitionWg, logger)
	}
	partitionWg.Wait()
	logger.Info("Client finished", zap.Int("clientNumber", i))
}

func produce(i int, partitionName string, conn *grpc.ClientConn, partitionWg *sync.WaitGroup, logger *zap.Logger) {
	defer partitionWg.Done()
	client := pb.NewBrokerClient(conn)
	md := metadata.New(map[string]string{server.PartitionNameMetadataKey: partitionName})
	stream, err := client.Produce(metadata.NewOutgoingContext(context.Background(), md))
	if err != nil {
		logger.Error("Failed to issue produce", zap.Int("clientNumber", i), zap.String("partitionName", partitionName))
		return
	}
	logger.Info("Issued produce call", zap.Int("clientNumber", i), zap.String("partitionName", partitionName))
	numberWritten := make(chan int)
	numberAcks := 0
	go produceMessages(stream, i, partitionName, numberWritten, logger)
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("Error receiving acks", zap.Error(err))
			break
		}
		logger.Info("Received ack", zap.Int("batchId", int(in.BatchId)), zap.String("partitionName", partitionName))
		numberAcks++
	}
	logger.Info("Finished produce call", zap.Int("clientNumber", i), zap.String("partitionName", partitionName), zap.Int("numberWritten", <-numberWritten), zap.Int("numberAcks", numberAcks))
}

var numberMessagesPerBatch []int = []int{3, 8, 2, 8}
var message []byte = []byte{0, 6, 100, 23}

func produceMessages(stream pb.Broker_ProduceClient, i int, partitionName string, numberWritten chan<- int, logger *zap.Logger) {
	for batchNumber, numberMessages := range numberMessagesPerBatch {
		messages := [][]byte{}
		for n := 0; n < numberMessages; n++ {
			messages = append(messages, message)
		}
		protoMessages := pb.Messages{
			Messages: messages,
		}
		batch := pb.MessageBatch{
			BatchId:  uint64(batchNumber + 1),
			Messages: &protoMessages,
		}
		if err := stream.Send(&batch); err != nil {
			logger.Error("Failed to send batch", zap.Int("clientNumber", i), zap.String("partitionName", partitionName), zap.Int("batchId", batchNumber+1), zap.Error(err))
			stream.CloseSend()
			numberWritten <- batchNumber
			return
		}
		logger.Info("Produced batch", zap.Int("clientNumber", i), zap.String("partitionName", partitionName), zap.Int("batchId", batchNumber+1))
	}
	stream.CloseSend()
	numberWritten <- len(numberMessagesPerBatch)
}
