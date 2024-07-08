package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Consumer struct {
	client                      *Client
	conn                        net.Conn
	logger                      *zap.Logger
	partitionName               string
	s3ObjectNames               []string
	currentObjectNextOffset     uint64
	NextOffsetToReturn          uint64
	currentObject               *minio.Object
	currentBatch                [][]byte
	endOfSafeOffsetsExclusively atomic.Uint64
	NewS3ObjectNames            chan []string
	objectStorageClient         *minio.Client
}

func (client *Client) NewConsumer(partitionName string, startOffset uint64) (*Consumer, error) {
	client.consumersRWMutex.Lock()
	c, ok := client.consumers[partitionName]
	if !ok {
		c = &Consumer{
			client:             client,
			conn:               client.conn,
			logger:             client.logger,
			partitionName:      partitionName,
			NewS3ObjectNames:   make(chan []string),
			NextOffsetToReturn: startOffset,
			s3ObjectNames:      make([]string, 0),
		}
		client.consumers[partitionName] = c
	}
	if client.objectStorageClient == nil {
		objectStorageClient, err := minioClient()
		if err != nil {
			return nil, fmt.Errorf("failed to make minio client: %v", err)
		}
		client.objectStorageClient = objectStorageClient
	}
	client.consumersRWMutex.Unlock()
	c.objectStorageClient = client.objectStorageClient
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

func (c *Consumer) ensureMinioObjectNames() error {
	if len(c.s3ObjectNames) != 0 {
		return nil
	}
	endOffsetExclusively := c.endOfSafeOffsetsExclusively.Load()
	c.logger.Info("Querying next minio object names", zap.String("partitionName", c.partitionName), zap.Uint64("startOffset", c.NextOffsetToReturn), zap.Uint64("endOffsetExclusively", endOffsetExclusively))
	for c.endOfSafeOffsetsExclusively.Load() <= c.NextOffsetToReturn {
		time.Sleep(10 * time.Microsecond)
	}
	req := &pb.Request{
		Request: &pb.Request_LogConsumeRequest{
			LogConsumeRequest: &pb.LogConsumeRequest{
				StartOffset:          c.NextOffsetToReturn,
				EndOffsetExclusively: c.endOfSafeOffsetsExclusively.Load(),
				PartitionName:        c.partitionName,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal log consume request: %v", err)
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send log consume req: %v", err)
	}
	c.s3ObjectNames = <-c.NewS3ObjectNames
	c.logger.Info("Learned about minio objects", zap.String("partitionName", c.partitionName), zap.Strings("objectNames", c.s3ObjectNames))
	if len(c.s3ObjectNames) == 0 {
		return errors.New("server returned empty list of s3 objects")
	}
	return nil
}

func (c *Consumer) ensureMinioObject() error {
	if c.currentObject != nil {
		return nil
	}
	c.logger.Info("Downloading next minio object", zap.String("partitionName", c.partitionName))
	err := c.ensureMinioObjectNames()
	if err != nil {
		return fmt.Errorf("failed to ensure minio object names present: %v", err)
	}
	nextS3Object := c.s3ObjectNames[0]
	c.s3ObjectNames = c.s3ObjectNames[1:]
	object, err := c.objectStorageClient.GetObject(context.TODO(), c.partitionName, nextS3Object, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download object from s3: %v", err)
	}
	c.currentObject = object
	stats, err := c.currentObject.Stat()
	if err != nil {
		return fmt.Errorf("failed to get current object stats: %v", err)
	}
	if stats.Size == 0 {
		c.currentObject = nil
		return fmt.Errorf("downloaded object %s of size 0", nextS3Object)
	} else {
		c.logger.Info("Downloaded object", zap.String("partitionName", c.partitionName), zap.String("objectName", nextS3Object), zap.Int64("objectSize", stats.Size))
		currentObjectStartOffset, err := strconv.Atoi(nextS3Object)
		if err != nil {
			return fmt.Errorf("failed to get starting offset of current object: %v", err)
		}
		c.currentObjectNextOffset = uint64(currentObjectStartOffset)
	}
	return nil
}

func (c *Consumer) ensureCurrentBatch() error {
	if c.currentBatch != nil && len(c.currentBatch) != 0 {
		return nil
	}
	c.currentBatch = nil
	for c.currentBatch == nil {
		err := c.ensureMinioObject()
		if err != nil {
			return fmt.Errorf("failed to ensure minio object is present locally: %v", err)
		}
		c.logger.Info("Unmarshaling next batch", zap.String("partitionName", c.partitionName))
		var messages pb.Messages
		err = protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.currentObject}, &messages)
		if err == io.EOF {
			c.logger.Info("EOF while reading batch from s3 object", zap.String("partitionName", c.partitionName))
			c.currentObject = nil
			continue
		} else if err != nil {
			return fmt.Errorf("failed to unmarshal next message batch: %v", err)
		}
		c.logger.Info("Read batch from s3 object", zap.String("partitionName", c.partitionName))
		if len(messages.Messages) == 0 {
			return errors.New("unmarshaled empty batch")
		}
		c.currentBatch = messages.Messages
	}
	return nil
}

// consumeNext returns the next available message regardless of c.NextOffsetToReturn
func (c *Consumer) consumeNext() ([]byte, error) {
	err := c.ensureCurrentBatch()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure batch locally present: %v", err)
	}
	result := c.currentBatch[0]
	c.currentBatch = c.currentBatch[1:]
	c.currentObjectNextOffset++
	return result, nil
}

// Consume returns the message with index c.NextOffsetToReturn
func (c *Consumer) Consume() ([]byte, error) {
	c.logger.Info("Consume called", zap.String("partitionName", c.partitionName))
	var result []byte
	for c.currentObjectNextOffset <= c.NextOffsetToReturn {
		var err error
		result, err = c.consumeNext()
		if err != nil {
			return nil, fmt.Errorf("failed to get next message: %v", err)
		}
	}
	c.NextOffsetToReturn++
	c.logger.Info("Returning message at the front of current batch", zap.String("partitionName", c.partitionName), zap.String("message", string(result)))
	return result, nil
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))

	c.client.consumersRWMutex.Lock()
	delete(c.client.consumers, c.partitionName)
	c.client.consumersRWMutex.Unlock()
	return nil
}
