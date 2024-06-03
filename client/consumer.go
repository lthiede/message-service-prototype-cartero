package client

import (
	"bytes"
	"fmt"
	"net"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Consumer struct {
	client                         *Client
	conn                           net.Conn
	logger                         *zap.Logger
	partitionName                  string
	endOfSafeOffsetsExclusively    uint64
	EndOfSafeOffsetsExclusivelyIn  chan uint64
	EndOfSafeOffsetsExclusivelyOut chan uint64
	Error                          chan error
	done                           chan struct{}
}

func (client *Client) NewConsumer(partitionName string, startOffset uint64) (*Consumer, error) {
	client.consumersRWMutex.Lock()
	c, ok := client.consumers[partitionName]
	if !ok {
		c = &Consumer{
			client:                         client,
			conn:                           client.conn,
			logger:                         client.logger,
			partitionName:                  partitionName,
			EndOfSafeOffsetsExclusivelyIn:  make(chan uint64),
			EndOfSafeOffsetsExclusivelyOut: make(chan uint64),
			Error:                          make(chan error),
			done:                           make(chan struct{}),
		}
		go c.handleOutput()
		client.consumers[partitionName] = c
	}
	client.consumersRWMutex.Unlock()
	req := &pb.Request{
		Request: &pb.Request_ConsumeRequest{
			ConsumeRequest: &pb.ConsumeRequest{
				StartOffset:   startOffset,
				PartitionName: partitionName,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal register consumer request: %v", err)
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %v", err)
	}
	client.logger.Info("Registered consumer", zap.String("partitionName", partitionName), zap.Int("startOffset", int(startOffset)))
	return c, nil
}

func (c *Consumer) handleOutput() {
	c.logger.Info("Start handling safe consume offsets", zap.String("partitionName", c.partitionName))
	newOffset := false
	for {
		if newOffset {
			select {
			case <-c.done:
				c.logger.Info("Stop handling consumption of batches", zap.String("partitionName", c.partitionName))
				return
			case end := <-c.EndOfSafeOffsetsExclusivelyIn:
				c.logger.Info("Consumer received safe consume offset", zap.String("partitionName", c.partitionName), zap.Int("offset", int(end)))
				c.endOfSafeOffsetsExclusively = end
			case c.EndOfSafeOffsetsExclusivelyOut <- c.endOfSafeOffsetsExclusively:
				newOffset = false
			}
		} else {
			select {
			case <-c.done:
				c.logger.Info("Stop handling consumption of batches", zap.String("partitionName", c.partitionName))
				return
			case end := <-c.EndOfSafeOffsetsExclusivelyIn:
				c.logger.Info("Consumer received safe consume offset", zap.String("partitionName", c.partitionName), zap.Int("offset", int(end)))
				c.endOfSafeOffsetsExclusively = end
				newOffset = true
			}
		}
	}
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))

	c.client.consumersRWMutex.Lock()
	delete(c.client.consumers, c.partitionName)
	c.client.consumersRWMutex.Unlock()
	close(c.done)
	return nil
}
