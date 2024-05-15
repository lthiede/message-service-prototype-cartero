package client

import (
	"context"
	"errors"
	"io"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type Consumer struct {
	cancel        context.CancelFunc
	context       context.Context
	logger        *zap.Logger
	stream        pb.Broker_ConsumeClient
	partitionName string
	offset        uint64
	messages      [][]byte
	Output        chan ConsumeOutput
	Error         chan error
}

type ConsumeOutput struct {
	Message []byte
	Offset  uint64
}

func (client *Client) NewConsumer(partitionName string, startOffset uint64) (*Consumer, error) {
	context, cancel := context.WithCancel(context.Background())
	md := metadata.New(map[string]string{server.PartitionNameMetadataKey: partitionName})
	stream, err := client.grpcClient.Consume(metadata.NewOutgoingContext(context, md))
	if err != nil {
		client.logger.Error("Failed to issue consume", zap.String("partitionName", partitionName))
		cancel()
		return nil, err
	}
	client.logger.Info("Issued consume call", zap.String("partitionName", partitionName))
	err = stream.Send(&pb.ConsumeRequest{
		StartOffset: uint64(startOffset),
	})
	if err == io.EOF {
		client.logger.Error("Failed to set start offset due to server side error", zap.String("partitionName", partitionName), zap.Int("startOffset", int(startOffset)))
		cancel()
		return nil, io.EOF
	}
	if err != nil {
		client.logger.Error("Failed to set start offset", zap.String("partitionName", partitionName), zap.Int("startOffset", int(startOffset)))
		cancel()
		return nil, err
	}
	client.logger.Info("Specified consume start offset", zap.String("partitionName", partitionName), zap.Int("startOffset", int(startOffset)))
	c := &Consumer{
		cancel:        cancel,
		context:       context,
		logger:        client.logger,
		stream:        stream,
		partitionName: partitionName,
		offset:        startOffset,
		Output:        make(chan ConsumeOutput),
		Error:         make(chan error),
	}
	go c.handleOutput()
	return c, nil
}

func (c *Consumer) SetStartOffset(startOffset uint64) error {
	err := c.stream.Send(&pb.ConsumeRequest{StartOffset: startOffset})
	if err == io.EOF {
		c.logger.Error("Failed to set start offset due to server side error", zap.String("partitionName", c.partitionName), zap.Int("startOffset", int(startOffset)))
		return io.EOF
	}
	if err != nil {
		c.logger.Error("Failed to set start offset", zap.String("partitionName", c.partitionName), zap.Int("startOffset", int(startOffset)))
		return err
	}
	c.logger.Info("Set new start offset", zap.String("partitionName", c.partitionName), zap.Int("startOffset", int(startOffset)))
	c.offset = startOffset
	c.messages = nil
	return nil
}

func (c *Consumer) handleOutput() {
	c.logger.Info("Start handling consumption of messages", zap.String("partitionName", c.partitionName))
	done := c.context.Done()
	for {
		select {
		case <-done:
			c.logger.Info("Context has been canceled. Stop handling consumption of messages", zap.String("partitionName", c.partitionName))
			return
		default:
			if len(c.messages) > 0 {
				message := c.messages[0]
				c.messages = c.messages[1:]
				currentOffset := c.offset
				c.offset++
				c.logger.Info("Returning message", zap.String("partitionName", c.partitionName), zap.Int("offset", int(currentOffset)))
				c.Output <- ConsumeOutput{
					Message: message,
					Offset:  currentOffset,
				}
				continue
			}
			in, err := c.stream.Recv()
			if err == io.EOF {
				c.logger.Info("Stream closed", zap.String("partitionName", c.partitionName))
				c.Error <- io.EOF
				c.Close()
				return
			}
			if err != nil {
				c.logger.Error("Failed to receive message", zap.String("partitionName", c.partitionName), zap.Error(err))
				c.Error <- err
				c.Close()
				return
			}
			if in.IsRedirectResponse {
				c.logger.DPanic("Can't read from broker cache. Reading directly from log instead", zap.String("partitionName", c.partitionName))
				c.Error <- errors.New("have to read directly from log")
				continue
			}
			messages := in.ConsumeMessageBatch.Messages.Messages
			message := messages[0]
			c.messages = messages[1:]
			currentOffset := in.ConsumeMessageBatch.StartOffset
			c.offset = currentOffset + 1
			c.logger.Info("Returning message after reading new batch", zap.String("partitionName", c.partitionName), zap.Int("offset", int(currentOffset)))
			c.Output <- ConsumeOutput{
				Message: message,
				Offset:  currentOffset,
			}
		}
	}
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))
	c.cancel()
	return nil
}
