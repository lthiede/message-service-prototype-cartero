package client

import (
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
	conn                            net.Conn
	connWriteMutex                  sync.Mutex
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
	return NewWithOptions(address, "" /*localAddr*/, logger)
}

func NewWithOptions(address string, localAddr string, logger *zap.Logger) (*Client, error) {
	dialer := &net.Dialer{}
	if localAddr != "" {
		dialer.LocalAddr = &net.TCPAddr{
			IP:   net.ParseIP(localAddr),
			Port: 0,
		}
		logger.Info("Using local address", zap.String("localAddress", localAddr))
	}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}
	logger.Info("Dialed server", zap.String("address", address))
	client := &Client{
		logger:                     logger,
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
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, response)
			if err != nil {
				c.logger.Error("Failed to unmarshal response", zap.Error(err))
				return
			}
			switch res := response.Response.(type) {
			case *pb.Response_ProduceAck:
				produceAck := res.ProduceAck
				c.producersRWMutex.RLock()
				p, ok := c.producers[produceAck.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", produceAck.PartitionName))
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

func (c *Client) Close() error {
	c.logger.Info("Closing client")
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
