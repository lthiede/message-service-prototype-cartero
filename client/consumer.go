package client

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Consumer struct {
	client                      *Client
	conn                        net.Conn
	logger                      *zap.Logger
	partitionName               string
	endOfSafeOffsetsExclusively atomic.Uint64
}

func (client *Client) NewConsumer(partitionName string, startOffset uint64) (*Consumer, error) {
	client.consumersRWMutex.Lock()
	c, ok := client.consumers[partitionName]
	if !ok {
		c = &Consumer{
			client:        client,
			conn:          client.conn,
			logger:        client.logger,
			partitionName: partitionName,
		}
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

func (c *Consumer) UpdateEndOfSafeOffsetsExclusively(endOfSafeOffsetsExclusively uint64) {
	c.endOfSafeOffsetsExclusively.Store(endOfSafeOffsetsExclusively)
}

func (c *Consumer) EndOfSafeOffsetsExclusively() uint64 {
	return c.endOfSafeOffsetsExclusively.Load()
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))

	c.client.consumersRWMutex.Lock()
	delete(c.client.consumers, c.partitionName)
	c.client.consumersRWMutex.Unlock()
	return nil
}
