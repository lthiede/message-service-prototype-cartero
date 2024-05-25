package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/lthiede/cartero/experiments/codec"
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
	return NewWithOptions(address, "", "", true, logger)
}

func NewWithOptions(address string, localAddr string, codecName string, noDelay bool, logger *zap.Logger) (*Client, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if codecName != "" {
		logger.Info("Using codec", zap.String("codec", codecName), zap.Bool("didInitRun", codec.DidInitRun))
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName)))
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(&codec.TestCodec{})))
	}
	if localAddr != "" {
		dialer := &net.Dialer{
			LocalAddr: &net.TCPAddr{
				IP:   net.ParseIP(localAddr),
				Port: 0,
			},
		}
		logger.Info("Using local address", zap.String("localAddress", localAddr))
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				return nil, errors.New("error casting connection to conn to *net.TCPConn")
			}
			tcpConn.SetNoDelay(noDelay)
			return tcpConn, nil
		}))
	}
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
