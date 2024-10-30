package client

import (
	"bytes"
	"fmt"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

func (c *Client) CreatePartition(partitionName string) error {
	successChan := make(chan bool)
	_, ok := c.expectedCreatePartitionRes[partitionName]
	if ok {
		return fmt.Errorf("already create partition request for %s in flight", partitionName)
	}
	c.expectedCreatePartitionRes[partitionName] = successChan
	req := &pb.Request{
		Request: &pb.Request_CreatePartitionRequest{
			CreatePartitionRequest: &pb.CreatePartitionRequest{
				PartitionName: partitionName,
			},
		},
	}
	c.logger.Info("Sending create partition request", zap.String("partitionName", partitionName))
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal create partition request, partition %s: %v", partitionName, err)
	}
	_, err = c.Conn.Write(wireMessage.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send create partition request, partition %s: %v", partitionName, err)
	}
	successful := <-successChan
	delete(c.expectedCreatePartitionRes, partitionName)
	if !successful {
		return fmt.Errorf("creating partition %s failed on the server side", partitionName)
	}
	return nil
}
