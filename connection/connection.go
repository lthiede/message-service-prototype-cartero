package connection

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sync"

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
	alive              bool
	aliveLock          sync.Mutex
	quit               chan struct{}
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
		alive:              true,
		quit:               make(chan struct{}),
	}
	go c.handleRequests()
	go c.handleResponses()
	logger.Info("New connection", zap.String("name", name))
	return c, nil
}

func (c *Connection) handleRequests() {
outer_loop:
	for {
		select {
		case <-c.quit:
			c.logger.Info("Stop handling requests", zap.String("name", c.name))
			return
		default:
			request := &pb.Request{}
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, request)
			if err != nil {
				c.logger.Error("Failed to unmarshal message", zap.Error(err), zap.String("name", c.name))
				c.Close()
				continue
			}
			switch req := request.Request.(type) {
			case *pb.Request_ProduceRequest:
				produceReq := req.ProduceRequest
				numMessages := uint32(len(produceReq.EndOffsetsExclusively))
				numBytes := produceReq.EndOffsetsExclusively[numMessages-1]
				// c.logger.Info("Received produce request", zap.Uint32("numMessages", numMessages), zap.Uint32("numBytes", numBytes), zap.String("name", c.name))
				payload := make([]byte, numBytes)
				for i := 0; i < int(numBytes); {
					n, err := c.conn.Read(payload[i:])
					if err != nil {
						c.logger.Error("Error reading produce payload", zap.Error(err), zap.String("name", c.name))
						c.Close()
						continue outer_loop
					}
					i += n
				}
				usablePartition := false
				// check cache for suitable partition
				p, ok := c.partitionCache[produceReq.PartitionName]
				if ok {
					p.AliveLock.RLock()
					if !p.Alive {
						delete(c.partitionCache, produceReq.PartitionName)
						p.AliveLock.RUnlock()
					} else {
						usablePartition = true
					}
				}
				// try to get one from partition manager
				if !usablePartition {
					p, err = c.partitionManager.GetPartition(produceReq.PartitionName)
					if err != nil {
						c.logger.Error("Error getting partition", zap.Error(err), zap.String("name", c.name))
						continue
					}
					p.AliveLock.RLock()
					if !p.Alive {
						p.AliveLock.RUnlock()
					} else {
						usablePartition = true
						c.partitionCache[produceReq.PartitionName] = p
					}
				}
				if usablePartition {
					p.LogInteractionRequests <- partition.LogInteractionRequest{
						AppendBatchRequest: &partition.AppendBatchRequest{
							BatchId:               produceReq.BatchId,
							EndOffsetsExclusively: produceReq.EndOffsetsExclusively,
							Payload:               payload,
							ProduceResponse:       c.responses,
						},
					}
					p.AliveLock.RUnlock()
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
				newPc, err := consume.NewPartitionConsumer(p, c.SendResponse, consumeReq.StartLsn, int(consumeReq.MinNumMessages), c.logger)
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
				err = c.SendResponse(response)
				if err != nil {
					c.logger.Error("Failed to send create partition response", zap.Error(err), zap.String("name", c.name))
					c.Close()
					continue
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
				err = c.SendResponse(response)
				if err != nil {
					c.logger.Error("Failed to send delete partition response", zap.Error(err), zap.String("name", c.name))
					c.Close()
					continue
				}
			default:
				c.logger.Error("Request type not recognized", zap.String("name", c.name))
				c.Close()
				continue
			}
		}
	}
}

func (c *Connection) SendResponse(res *pb.Response) error {
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
		err := c.SendResponse(response)
		if err != nil {
			c.logger.Error("Failed to asynchronously send response", zap.Error(err),
				zap.String("name", c.name))
			c.Close()
			continue
		}
	}
}

func (c *Connection) Close() error {
	c.aliveLock.Lock()
	defer c.aliveLock.Unlock()
	if !c.alive {
		return nil
	}
	c.logger.Info("Closing connection", zap.String("name", c.name))
	c.alive = false
	close(c.responses)
	close(c.quit)
	for _, pc := range c.partitionConsumers {
		return pc.Close()
	}
	return nil
}
