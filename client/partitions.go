package client

import (
	"bytes"
	"fmt"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

func (c *Client) CreatePartition(partitionName string, numPartitions uint32) error {
	successChan := make(chan bool)
	c.expectedCreatePartitionResMutex.Lock()
	_, ok := c.expectedCreatePartitionRes[partitionName]
	if ok {
		c.expectedCreatePartitionResMutex.Unlock()
		return fmt.Errorf("already create partition request for %s in flight", partitionName)
	}
	c.expectedCreatePartitionRes[partitionName] = successChan
	c.expectedCreatePartitionResMutex.Unlock()
	req := &pb.Request{
		Request: &pb.Request_CreatePartitionRequest{
			CreatePartitionRequest: &pb.CreatePartitionRequest{
				TopicName:     partitionName,
				NumPartitions: numPartitions,
			},
		},
	}
	c.logger.Info("Sending create partition request", zap.String("partitionName", partitionName))
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal create partition request, partition %s: %v", partitionName, err)
	}
	request := wireMessage.Bytes()
	sentSuccessfully := false
	for !sentSuccessfully {
		c.epochMutex.RLock()
		err := c.sendBytesOverNetwork(request)
		potentialFailureEpoch := c.epoch
		c.epochMutex.RUnlock()
		if err != nil {
			c.logger.Error("Failed to send delete partition request", zap.Error(err))
			err := c.restoreConnection(potentialFailureEpoch, "partitions")
			if err != nil {
				return fmt.Errorf("failed to recover from network failure: %v", err)
			}
		} else {
			sentSuccessfully = true
		}
	}
	successful := <-successChan
	if !successful {
		return fmt.Errorf("creating partition %s failed on the server side", partitionName)
	} else {
		c.logger.Info("Successfully created partition")
	}
	return nil
}

func (c *Client) DeletePartition(partitionName string, numPartitions uint32) error {
	successChan := make(chan bool)
	c.expectedDeletePartitionResMutex.Lock()
	_, ok := c.expectedDeletePartitionRes[partitionName]
	if ok {
		c.expectedDeletePartitionResMutex.Unlock()
		return fmt.Errorf("already delete partition request for %s in flight", partitionName)
	}
	c.expectedDeletePartitionRes[partitionName] = successChan
	c.expectedDeletePartitionResMutex.Unlock()
	req := &pb.Request{
		Request: &pb.Request_DeletePartitionRequest{
			DeletePartitionRequest: &pb.DeletePartitionRequest{
				TopicName:     partitionName,
				NumPartitions: numPartitions,
			},
		},
	}
	c.logger.Info("Sending delete partition request", zap.String("partitionName", partitionName))
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal delete partition request, partition %s: %v", partitionName, err)
	}
	request := wireMessage.Bytes()
	sentSuccessfully := false
	for !sentSuccessfully {
		c.epochMutex.RLock()
		err := c.sendBytesOverNetwork(request)
		potentialFailureEpoch := c.epoch
		c.epochMutex.RUnlock()
		if err != nil {
			c.logger.Error("Failed to send delete partition request", zap.Error(err))
			err := c.restoreConnection(potentialFailureEpoch, "partitions")
			if err != nil {
				return fmt.Errorf("failed to recover from network failure: %v", err)
			}
		} else {
			sentSuccessfully = true
		}
	}
	successful := <-successChan
	if !successful {
		return fmt.Errorf("deleting partition %s failed on the server side", partitionName)
	}
	return nil
}
