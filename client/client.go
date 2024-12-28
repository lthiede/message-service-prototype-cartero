package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Client struct {
	logger                          *zap.Logger
	address                         string
	conn                            net.Conn
	connWriteMutex                  sync.Mutex
	epoch                           uint64
	epochMutex                      sync.RWMutex
	failed                          bool
	producers                       map[string]*Producer
	producersRWMutex                sync.RWMutex
	consumers                       map[string]*Consumer
	consumersRWMutex                sync.RWMutex
	expectedCreatePartitionRes      map[string]chan bool
	expectedCreatePartitionResMutex sync.Mutex
	expectedDeletePartitionRes      map[string]chan bool
	expectedDeletePartitionResMutex sync.Mutex
	done                            chan struct{}
}

func New(address string, logger *zap.Logger) (*Client, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}
	logger.Info("Dialed server", zap.String("address", address))
	client := &Client{
		logger:                     logger,
		address:                    address,
		conn:                       conn,
		producers:                  map[string]*Producer{},
		consumers:                  map[string]*Consumer{},
		expectedCreatePartitionRes: map[string]chan bool{},
		expectedDeletePartitionRes: map[string]chan bool{},
		done:                       make(chan struct{}),
	}
	go client.handleResponses()
	return client, nil
}

func (c *Client) handleResponses() {
	for {
		select {
		case <-c.done:
			c.logger.Info("Stop handling responses")
			return
		default:
			response := &pb.Response{}
			c.epochMutex.RLock()
			potentialFailureEpoch := c.epoch
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, response)
			c.epochMutex.RUnlock()
			if err != nil {
				c.logger.Error("Failed to unmarshal response", zap.Error(err))
				select {
				case <-c.done:
					c.logger.Info("Stop handling responses")
					return
				default:
					err := c.restoreConnection(potentialFailureEpoch, "client")
					if err != nil {
						c.logger.Error("Failed to restore connection", zap.Error(err))
						return
					}
					continue
				}
			}
			switch res := response.Response.(type) {
			case *pb.Response_ProduceAck:
				produceAck := res.ProduceAck
				c.producersRWMutex.RLock()
				p, ok := c.producers[produceAck.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", produceAck.PartitionName))
					c.producersRWMutex.RUnlock()
					continue
				}
				p.UpdateAcknowledged(produceAck)
				c.producersRWMutex.RUnlock()
			case *pb.Response_ConsumeResponse:
				consumeRes := res.ConsumeResponse
				c.consumersRWMutex.RLock()
				cons, ok := c.consumers[consumeRes.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", consumeRes.PartitionName))
					c.consumersRWMutex.RUnlock()
					continue
				}
				// c.logger.Info("Client received safe consume offset", zap.String("partitionName", consumeRes.PartitionName), zap.Int("endOfSafeLSNsExclusively", int(consumeRes.EndOfSafeLsnsExclusively)))
				cons.UpdateEndOfSafeLSNsExclusively(consumeRes.EndOfSafeLsnsExclusively)
				c.consumersRWMutex.RUnlock()
			case *pb.Response_CreatePartitionResponse:
				createPartitionRes := res.CreatePartitionResponse
				c.expectedCreatePartitionResMutex.Lock()
				successChan, ok := c.expectedCreatePartitionRes[createPartitionRes.PartitionName]
				if !ok {
					c.logger.Error("Received a create partition response but not waiting for one", zap.String("partitionName", createPartitionRes.PartitionName))
					continue
				}
				delete(c.expectedCreatePartitionRes, createPartitionRes.PartitionName)
				c.expectedCreatePartitionResMutex.Unlock()
				successChan <- createPartitionRes.Successful
			case *pb.Response_DeletePartitionResponse:
				deletePartitionRes := res.DeletePartitionResponse
				c.expectedDeletePartitionResMutex.Lock()
				successChan, ok := c.expectedDeletePartitionRes[deletePartitionRes.PartitionName]
				if !ok {
					c.logger.Error("Received a delete partition response but not waiting for one", zap.String("partitionName", deletePartitionRes.PartitionName))
				}
				delete(c.expectedDeletePartitionRes, deletePartitionRes.PartitionName)
				c.expectedDeletePartitionResMutex.Unlock()
				successChan <- deletePartitionRes.Successful
			default:
				c.logger.Error("Request type not recognized")
			}
		}
	}
}

func (c *Client) sendBytesOverNetwork(request []byte) error {
	c.connWriteMutex.Lock()
	defer c.connWriteMutex.Unlock()
	_, err := c.conn.Write(request)
	if err != nil {
		return fmt.Errorf("failed to send bytes over network: %v", err)
	}
	return nil
}

func (c *Client) restoreConnection(failureEpoch uint64, calledFrom string) error {
	c.epochMutex.Lock()
	defer c.epochMutex.Unlock()
	c.logger.Info("Got epoch mutex", zap.String("calledFrom", calledFrom))
	c.connWriteMutex.Lock()
	defer c.connWriteMutex.Unlock()
	c.logger.Info("Got conn mutex", zap.String("calledFrom", calledFrom))
	if failureEpoch < c.epoch {
		c.logger.Info("Don't need to restore due to epoch")
		return nil
	}
	c.logger.Info("Trying to restore connection")
	if c.failed {
		return errors.New("restoring connection already failed")
	}
	dialer := &net.Dialer{}
	conn, err := dialer.Dial("tcp", c.address)
	if err != nil {
		c.failed = true
		return fmt.Errorf("failed restoring connection: %v", err)
	}
	c.logger.Info("Restored connection")
	c.epoch++
	c.conn = conn
	return nil
}

func (c *Client) Close() error {
	c.logger.Info("Closing client")
	close(c.done)
	for _, producer := range c.producers {
		err := producer.Close()
		if err != nil {
			return fmt.Errorf("failed to close producer: %v", err)
		}
	}
	for _, consumer := range c.consumers {
		err := consumer.Close()
		if err != nil {
			return fmt.Errorf("failed to close producer: %v", err)
		}
	}
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
