package client

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
Kafka Java:
Separate Producer and Consumer
Producer.send(ProducerRecord(topic, key, value), callback)
Consumer.subscribe(topics)
ConsumerRecords = Consumer.poll()

Kafka Confluent Go:
Separate Producer and Consumer
Producer.send(ProducerRecord(topic, key, value))
Event chan for acks
Consumer.subscribe(topics)
ConsumerRecords = Consumer.poll()

Kafka Sarama:
Separate Producer and Consumer
AsyncProducer receives ProducerMessage(topic, value) on Input chan
Error and Success chan
partition, offset, err = SyncProducer.SendMessage(ProducerMessage(topic, value))
partitionConsumer = consumePartition(partition)
chan for messages
*/

type Client struct {
	logger     *zap.Logger
	conn       *grpc.ClientConn
	grpcClient pb.BrokerClient
}

func New(address string, logger *zap.Logger) (*Client, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}
	logger.Info("Dialed server", zap.String("address", address))
	grpcClient := pb.NewBrokerClient(conn)
	return &Client{
		logger:     logger,
		conn:       conn,
		grpcClient: grpcClient,
	}, nil
}

func (c *Client) PingPong() error {
	_, err := c.grpcClient.PingPong(context.Background(), &pb.Ping{})
	return err
}

func (c *Client) TimedPingPong() (time.Duration, error) {
	start := time.Now()
	_, err := c.grpcClient.PingPong(context.Background(), &pb.Ping{})
	end := time.Now()
	if err != nil {
		return 0, err
	}
	return end.Sub(start), nil
}

func (c *Client) Close() error {
	moreImportantErr := c.conn.Close()
	if moreImportantErr != nil {
		c.logger.Error("Failed to close connection", zap.Error(moreImportantErr))
	}
	lessImportantErr := c.logger.Sync()
	if lessImportantErr != nil {
		log.Printf("Failed to sync logger: %v", lessImportantErr)
	}
	if moreImportantErr != nil {
		return moreImportantErr
	} else {
		return lessImportantErr
	}
}
