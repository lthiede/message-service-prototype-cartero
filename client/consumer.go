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

var Concurrency int = 16

type Consumer struct {
	client                   *Client
	conn                     net.Conn
	logger                   *zap.Logger
	partitionName            string
	nextOffsetToReturn       uint64
	endOfSafeLSNsExclusively atomic.Uint64
	done                     chan struct{}
}

func (client *Client) NewConsumer(partitionName string, startLSN uint64) (*Consumer, error) {
	client.consumersRWMutex.Lock()
	c, ok := client.consumers[partitionName]
	if ok {
		client.logger.Info("Client already exists")
		return c, nil
	}
	consumer := &Consumer{
		client:        client,
		conn:          client.Conn,
		logger:        client.logger,
		partitionName: partitionName,
		done:          make(chan struct{}),
	}
	client.consumers[partitionName] = consumer
	client.consumersRWMutex.Unlock()
	req := &pb.Request{
		Request: &pb.Request_ConsumeRequest{
			ConsumeRequest: &pb.ConsumeRequest{
				StartLsn:      startLSN,
				PartitionName: partitionName,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal register consumer request: %v", err)
	}
	_, err = consumer.conn.Write(wireMessage.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %v", err)
	}
	client.logger.Info("Registered consumer", zap.String("partitionName", partitionName), zap.Int("startLSN", int(startLSN)))
	return consumer, nil
}

func (c *Consumer) UpdateEndOfSafeLSNsExclusively(endOfSafeLSNsExclusively uint64) {
	c.endOfSafeLSNsExclusively.Store(endOfSafeLSNsExclusively)
}

func (c *Consumer) EndOfSafeLSNsExclusively() uint64 {
	return c.endOfSafeLSNsExclusively.Load()
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))
	close(c.done)
	c.client.consumersRWMutex.Lock()
	delete(c.client.consumers, c.partitionName)
	c.client.consumersRWMutex.Unlock()
	return nil
}
