package connection

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"

	"github.com/lthiede/cartero/consume"
	"github.com/lthiede/cartero/partition"
	"github.com/lthiede/cartero/partitionmanager"
	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

var connectionNames []string = []string{
	"Herbert",
	"Dominik",
	"Melanie",
	"Berta",
	"Hans",
	"Ephraim",
	"David",
	"Lidia",
}

var connectionAdjectives []string = []string{
	"sexy",
	"cool",
	"moist",
	"clever",
	"terrible",
	"slow",
	"awesome",
	"draining",
}

type Connection struct {
	conn               net.Conn
	name               string
	partitionCache     map[string]*partition.Partition
	partitionManager   *partitionmanager.PartitionManager
	partitionConsumers map[string]*consume.PartitionConsumer
	responses          chan *pb.Response
	logger             *zap.Logger
}

func New(conn net.Conn, partitionManager *partitionmanager.PartitionManager, logger *zap.Logger) (*Connection, error) {
	connectionAdjective := connectionAdjectives[rand.Int()%len(connectionAdjectives)]
	connectionName := connectionNames[rand.Int()%len(connectionNames)]
	name := fmt.Sprintf("%s %s", connectionAdjective, connectionName)
	c := &Connection{
		conn:               conn,
		name:               name,
		partitionCache:     make(map[string]*partition.Partition),
		partitionManager:   partitionManager,
		partitionConsumers: make(map[string]*consume.PartitionConsumer),
		responses:          make(chan *pb.Response),
		logger:             logger,
	}
	go c.handleRequests()
	go c.handleResponses()
	logger.Info("New connection", zap.String("name", name))
	return c, nil
}

func (c *Connection) handleRequests() {
	for {
		request := &pb.Request{}
		err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, request)
		if err != nil {
			c.logger.Error("Failed to unmarshal message", zap.Error(err), zap.String("name", c.name))
			c.logger.Info("Stop handling requests", zap.String("name", c.name))
			c.Close()
			return
		}
		switch req := request.Request.(type) {
		case *pb.Request_ProduceRequest:
			produceReq := req.ProduceRequest
			numMessages := uint32(len(produceReq.EndOffsetsExclusively))
			numBytes := produceReq.EndOffsetsExclusively[numMessages-1]
			payload := make([]byte, numBytes)
			for i := 0; i < int(numBytes); {
				n, err := c.conn.Read(payload[i:])
				if err != nil {
					c.logger.Error("Error reading produce payload", zap.Error(err), zap.String("name", c.name))
					c.logger.Info("Stop handling requests", zap.String("name", c.name))
					c.Close()
					return
				}
				i += n
			}
			// check cache for suitable partition
			successful := false
			p, ok := c.partitionCache[produceReq.PartitionName]
			if ok {
				err := c.sendProduceRequest(produceReq, payload, p)
				if err != nil {
					c.logger.Error("Error sending produce request to cached partition",
						zap.Error(err),
						zap.String("partitionName", produceReq.PartitionName),
						zap.String("name", c.name))
					delete(c.partitionCache, produceReq.PartitionName)
				} else {
					successful = true
				}
			}
			// try to get one from partition manager
			if !successful {
				p, err := c.partitionManager.GetPartition(produceReq.PartitionName)
				if err != nil {
					c.logger.Error("Error getting partition",
						zap.Error(err),
						zap.String("partitionName", produceReq.PartitionName),
						zap.String("name", c.name))
					continue
				}
				err = c.sendProduceRequest(produceReq, payload, p)
				if err != nil {
					c.logger.Error("Error sending produce request to partition from partition manager",
						zap.Error(err),
						zap.String("partitionName", produceReq.PartitionName),
						zap.String("name", c.name))
				} else {
					c.partitionCache[produceReq.PartitionName] = p
				}
			}
		case *pb.Request_ConsumeRequest:
			consumeReq := req.ConsumeRequest
			p, ok := c.partitionCache[consumeReq.PartitionName]
			if !ok {
				p, err = c.partitionManager.GetPartition(consumeReq.PartitionName)
				if err != nil {
					c.logger.Error("Error getting partition", zap.Error(err), zap.String("name", c.name))
					continue
				}
			}
			pc, ok := c.partitionConsumers[consumeReq.PartitionName]
			if ok {
				err := pc.UpdateConsumption(consumeReq.StartLsn, int(consumeReq.MinNumMessages))
				if err != nil {
					continue
				}
			}
			newPc, err := consume.NewPartitionConsumer(p, c.sendResponse, consumeReq.StartLsn, int(consumeReq.MinNumMessages), c.logger)
			if err != nil {
				c.logger.Error("Failed to register consumer", zap.String("partitionName", consumeReq.PartitionName), zap.String("name", c.name))
				continue
			}
			c.partitionConsumers[consumeReq.PartitionName] = newPc
			c.logger.Info("Registered partition consumer", zap.String("partitionName", consumeReq.PartitionName), zap.String("name", c.name))
		case *pb.Request_CreatePartitionRequest:
			createPartitionRequest := req.CreatePartitionRequest
			c.logger.Info("Received create partition request", zap.String("name", c.name), zap.String("partitionName", createPartitionRequest.TopicName))
			err := c.partitionManager.CreatePartition(createPartitionRequest.TopicName, createPartitionRequest.NumPartitions)
			if err != nil {
				c.logger.Error("Failed to create partition", zap.Error(err), zap.String("name", c.name))
			} else {
				c.logger.Info("Successfully created partition", zap.String("partitionName", createPartitionRequest.TopicName), zap.String("name", c.name))
			}
			response := &pb.Response{
				Response: &pb.Response_CreatePartitionResponse{
					CreatePartitionResponse: &pb.CreatePartitionResponse{
						PartitionName: createPartitionRequest.TopicName,
						Successful:    err == nil,
					},
				},
			}
			err = c.sendResponse(response)
			if err != nil {
				c.logger.Error("Failed to send create partition response", zap.Error(err), zap.String("name", c.name))
				c.Close()
				return
			}
		case *pb.Request_DeletePartitionRequest:
			deletePartitionRequest := req.DeletePartitionRequest
			c.logger.Info("Received delete partition request", zap.String("name", c.name), zap.String("partitionName", deletePartitionRequest.TopicName))
			err := c.partitionManager.DeletePartition(deletePartitionRequest.TopicName, deletePartitionRequest.NumPartitions)
			if err != nil {
				c.logger.Error("Failed to delete partition", zap.Error(err), zap.String("name", c.name))
			} else {
				c.logger.Info("Successfully deleted partition", zap.String("partitionName", deletePartitionRequest.TopicName), zap.String("name", c.name))
			}
			response := &pb.Response{
				Response: &pb.Response_DeletePartitionResponse{
					DeletePartitionResponse: &pb.DeletePartitionResponse{
						PartitionName: deletePartitionRequest.TopicName,
						Successful:    err == nil,
					},
				},
			}
			err = c.sendResponse(response)
			if err != nil {
				c.logger.Error("Failed to send delete partition response", zap.Error(err), zap.String("name", c.name))
				c.Close()
				return
			}
		default:
			c.logger.Error("Request type not recognized", zap.String("name", c.name))
			c.Close()
			return
		}
	}
}

func (c *Connection) sendProduceRequest(produceReq *pb.ProduceRequest, payload []byte, p *partition.Partition) (retErr error) {
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("Caught error",
				zap.Any("error", err))
			retErr = errors.New("caught panic sending produce request on channel")
		}
	}()
	p.LogInteractionRequests <- partition.LogInteractionRequest{
		AppendBatchRequest: &partition.AppendBatchRequest{
			BatchId:               produceReq.BatchId,
			EndOffsetsExclusively: produceReq.EndOffsetsExclusively,
			Payload:               payload,
			ProduceResponse:       c.responses,
		},
	}
	return nil
}

func (c *Connection) sendResponse(res *pb.Response) error {
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, res)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) handleResponses() {
	for {
		response, ok := <-c.responses
		if !ok {
			c.logger.Info("Stop handling responses", zap.String("name", c.name))
			return
		}
		err := c.sendResponse(response)
		if err != nil {
			c.logger.Error("Failed to asynchronously send response", zap.Error(err),
				zap.String("name", c.name))
			c.logger.Info("Stop handling responses", zap.String("name", c.name))
			return
		}
	}
}

func (c *Connection) Close() error {
	c.logger.Info("Closing connection", zap.String("name", c.name))
	close(c.responses)
	for _, pc := range c.partitionConsumers {
		return pc.Close()
	}
	return nil
}
