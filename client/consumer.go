package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

var Concurrency int = 16

type Consumer struct {
	client                      *Client
	conn                        net.Conn
	logger                      *zap.Logger
	partitionName               string
	downloadTasks               chan string
	objectNames                 []string
	objectNamesLock             sync.Mutex
	currentObjectNextOffset     uint64
	nextOffsetToReturn          uint64
	objects                     map[string]*objectInDownload
	objectsLock                 sync.Mutex
	endOfSafeOffsetsExclusively atomic.Uint64
	newS3ObjectNames            chan []string
	currentObject               *objectInDownload
	currentBatch                [][]byte
	nextBatchIndex              int
	objectStorageClient         *minio.Client
	done                        chan struct{}
}

type objectInDownload struct {
	complete bool
	batches  [][][]byte
	lock     sync.Mutex
}

func (client *Client) NewConsumer(partitionName string, startOffset uint64) (*Consumer, error) {
	client.consumersRWMutex.Lock()
	c, ok := client.consumers[partitionName]
	if ok {
		consumer := c.(*Consumer)
		if consumer == nil {
			client.logger.Info("Client already exists", zap.String("type", reflect.TypeOf(c).String()))
			return nil, fmt.Errorf("existing client for partition %s is not of type Consumer", partitionName)
		} else {
			client.logger.Info("Client already exists")
			return consumer, nil
		}
	}
	consumer := &Consumer{
		client:              client,
		conn:                client.conn,
		logger:              client.logger,
		partitionName:       partitionName,
		downloadTasks:       make(chan string),
		objects:             make(map[string]*objectInDownload),
		nextOffsetToReturn:  startOffset,
		objectStorageClient: client.objectStorageClient,
		newS3ObjectNames:    make(chan []string),
		done:                make(chan struct{}),
	}
	client.consumers[partitionName] = consumer
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
	_, err = consumer.conn.Write(wireMessage.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %v", err)
	}
	client.logger.Info("Registered consumer", zap.String("partitionName", partitionName), zap.Int("startOffset", int(startOffset)))
	consumer.logger.Info("Finding downloadable objects")
	go consumer.findDownloadableObjects(startOffset)
	for range Concurrency {
		go consumer.downloadObjects()
	}
	return consumer, nil
}

func (c *Consumer) FeedNewS3ObjectNames(s3ObjectNames []string) {
	c.newS3ObjectNames <- s3ObjectNames
}

func (c *Consumer) UpdateEndOfSafeOffsetsExclusively(endOfSafeOffsetsExclusively uint64) {
	c.endOfSafeOffsetsExclusively.Store(endOfSafeOffsetsExclusively)
}

func (c *Consumer) EndOfSafeOffsetsExclusively() uint64 {
	return c.endOfSafeOffsetsExclusively.Load()
}

const Timeout = 100 * time.Millisecond

var NextObjectNameTimeout = 100 * time.Millisecond

var ErrTimeout = errors.New("waiting for new messages timed out")

func (c *Consumer) findDownloadableObjects(startOffset uint64) {
	var s3ObjectNames []string
	offsetWantedInS3 := startOffset
	// we assume that endOfSafeOffsetsExclusively always corresponds to the start of a new object
	// why do we assume this? this seems wrong
	for {
		if len(s3ObjectNames) > 0 {
			select {
			case <-c.done:
				c.logger.Info("Stop looking for downloadable objects", zap.String("partitionName", c.partitionName))
				return
			case c.downloadTasks <- s3ObjectNames[0]:
				s3ObjectNames = s3ObjectNames[1:]
			}
		} else {
			select {
			case <-c.done:
				c.logger.Info("Stop looking for downloadable Objects", zap.String("partitionName", c.partitionName))
				return
			default:
				endOfSafeOffsetsExclusively := c.endOfSafeOffsetsExclusively.Load()
				if endOfSafeOffsetsExclusively <= offsetWantedInS3 {
					time.Sleep(10 * time.Microsecond)
					continue
				}
				c.logger.Info("Querying next minio object names", zap.String("partitionName", c.partitionName), zap.Uint64("startOffset", offsetWantedInS3), zap.Uint64("endOffsetExclusively", endOfSafeOffsetsExclusively))
				req := &pb.Request{
					Request: &pb.Request_LogConsumeRequest{
						LogConsumeRequest: &pb.LogConsumeRequest{
							StartOffset:          offsetWantedInS3,
							EndOffsetExclusively: endOfSafeOffsetsExclusively,
							PartitionName:        c.partitionName,
						},
					},
				}
				wireMessage := &bytes.Buffer{}
				_, err := protodelim.MarshalTo(wireMessage, req)
				if err != nil {
					c.logger.Error("Failed to marshal log consume request", zap.Error(err))
					continue
				}
				_, err = c.conn.Write(wireMessage.Bytes())
				if err != nil {
					c.logger.Error("Failed to send log consume req", zap.Error(err))
					continue
				}
				s3ObjectNames = <-c.newS3ObjectNames
				c.objectNamesLock.Lock()
				c.objectNames = append(c.objectNames, s3ObjectNames...)
				c.objectNamesLock.Unlock()
				offsetWantedInS3 = endOfSafeOffsetsExclusively
				c.logger.Info("Learned about minio objects", zap.String("partitionName", c.partitionName), zap.Strings("objectNames", s3ObjectNames))
			}
		}
	}
}

func (c *Consumer) downloadObjects() {
	for {
		select {
		case <-c.done:
			c.logger.Info("Stop downloading Objects", zap.String("partitionName", c.partitionName))
			return
		case downloadTask := <-c.downloadTasks:
			objectInDownload := objectInDownload{
				batches: make([][][]byte, 0),
			}
			c.objectsLock.Lock()
			c.objects[downloadTask] = &objectInDownload
			c.objectsLock.Unlock()
			object, err := c.objectStorageClient.GetObject(context.TODO(), c.partitionName, downloadTask, minio.GetObjectOptions{})
			if err != nil {
				c.logger.Error("Failed to download object from s3", zap.Error(err), zap.String("objectName", downloadTask))
				return
			}
			stats, err := object.Stat()
			if err != nil {
				c.logger.Error("Failed to get object stats", zap.Error(err), zap.String("objectName", downloadTask))
				return
			}
			if stats.Size == 0 {
				c.logger.Info("Downloaded object of size 0", zap.String("objectName", downloadTask))
				objectInDownload.lock.Lock()
				objectInDownload.complete = true
				objectInDownload.lock.Unlock()
				continue
			}
			var messages pb.Messages
			err = protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: object}, &messages)
			for ; err == nil; err = protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: object}, &messages) {
				if len(messages.Messages) == 0 {
					c.logger.Warn("Unmarshaled empty batch", zap.String("objectName", downloadTask))
					continue
				}
				objectInDownload.lock.Lock()
				objectInDownload.batches = append(objectInDownload.batches, messages.Messages)
				objectInDownload.lock.Unlock()
			}
			if err == io.EOF {
				objectInDownload.lock.Lock()
				objectInDownload.complete = true
				objectInDownload.lock.Unlock()
				continue
			}
			c.logger.Error("Failed to unmarshal next batch", zap.Error(err), zap.String("objectName", downloadTask))
			return
		}
	}
}

func (c *Consumer) nextObjectName() (string, error) {
	timeSlept := 0 * time.Microsecond
	for {
		c.objectNamesLock.Lock()
		if len(c.objectNames) > 0 {
			nextObject := c.objectNames[0]
			c.objectNamesLock.Unlock()
			return nextObject, nil
		} else {
			c.objectNamesLock.Unlock()
			time.Sleep(10 * time.Microsecond)
			timeSlept += 10 * time.Microsecond
			if timeSlept >= NextObjectNameTimeout {
				c.logger.Error("Waiting for next object name timed out", zap.Float64("secondsWaited", timeSlept.Seconds()), zap.Float64("timeout", NextObjectNameTimeout.Seconds()))
				return "", fmt.Errorf("failed to wait for next object name: %v", ErrTimeout)
			}
		}
	}
}

func (c *Consumer) nextObject() error {
	objectName, err := c.nextObjectName()
	if err == ErrTimeout {
		return err
	}
	if err != nil {
		return fmt.Errorf("failed to get next object name: %v", err)
	}
	timeSlept := 0 * time.Microsecond
	for {
		c.objectsLock.Lock()
		object, ok := c.objects[objectName]
		c.objectsLock.Unlock()
		if ok {
			c.currentObject = object
			c.nextBatchIndex = 0
			var err error
			c.currentObjectNextOffset, err = strconv.ParseUint(objectName, 10, 64)
			if err != nil {
				return fmt.Errorf("error determining current object starting offset from name: %v", err)
			}
			c.currentBatch = nil
			c.logger.Info("Starting next object", zap.String("objectName", objectName))
			return nil
		} else {
			time.Sleep(10 * time.Microsecond)
			timeSlept += 10 * time.Microsecond
			if timeSlept >= Timeout {
				c.logger.Error("Waiting for next object timed out", zap.String("objectName", objectName))
				return ErrTimeout
			}
		}
	}
}

func (c *Consumer) removeCurrentObject() {
	c.objectNamesLock.Lock()
	currentObjectName := c.objectNames[0]
	c.objectNames = c.objectNames[1:]
	c.objectNamesLock.Unlock()

	c.objectsLock.Lock()
	delete(c.objects, currentObjectName)
	c.objectsLock.Unlock()

	// only deleted after complete
	// no concurrent access possible
	c.currentObject = nil
}

func (c *Consumer) nextBatch() error {
	for {
		if c.currentObject == nil {
			err := c.nextObject()
			if err == ErrTimeout {
				return err
			}
			if err != nil {
				return fmt.Errorf("failed to get next object: %v", err)
			}
		}
		c.currentObject.lock.Lock()
		if !c.currentObject.complete {
			timeSlept := 0 * time.Microsecond
			for !(c.nextBatchIndex < len(c.currentObject.batches)) {
				c.currentObject.lock.Unlock()
				time.Sleep(10 * time.Microsecond)
				timeSlept += 10 * time.Microsecond
				if timeSlept >= Timeout {
					c.logger.Error("Waiting for new batch in current object timed out")
					return ErrTimeout
				}
				c.currentObject.lock.Lock()
			}
			c.currentBatch = c.currentObject.batches[c.nextBatchIndex]
			c.nextBatchIndex++
			c.currentObject.lock.Unlock()
			// c.logger.Info("Starting next batch")
			return nil
		}
		if c.nextBatchIndex < len(c.currentObject.batches) {
			c.currentObject.lock.Unlock()
			c.currentBatch = c.currentObject.batches[c.nextBatchIndex]
			c.nextBatchIndex++
			// c.logger.Info("Starting next batch")
			return nil
		}
		// the end of the current object is reached
		c.currentObject.lock.Unlock()
		c.removeCurrentObject()
	}
}

// consumeNext returns the next available message regardless of c.nextOffsetToReturn
func (c *Consumer) consumeNext() ([]byte, error) {
	for c.currentBatch == nil || len(c.currentBatch) == 0 {
		err := c.nextBatch()
		if err == ErrTimeout {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get next batch: %v", err)
		}
	}
	message := c.currentBatch[0]
	c.currentBatch = c.currentBatch[1:]
	c.currentObjectNextOffset++
	return message, nil
}

// Consume returns the message with index c.nextOffsetToReturn
func (c *Consumer) Consume() ([]byte, error) {
	// c.logger.Info("Consume called", zap.String("partitionName", c.partitionName))
	var result []byte
	for c.currentObjectNextOffset <= c.nextOffsetToReturn {
		var err error
		result, err = c.consumeNext()
		if err == ErrTimeout {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get next message: %v", err)
		}
	}
	c.nextOffsetToReturn++
	// c.logger.Info("Returning message at the front of current batch", zap.String("partitionName", c.partitionName), zap.String("message", string(result)))
	return result, nil
}

func (c *Consumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.partitionName))
	close(c.done)
	c.client.consumersRWMutex.Lock()
	delete(c.client.consumers, c.partitionName)
	c.client.consumersRWMutex.Unlock()
	return nil
}
