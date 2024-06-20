package connection

import (
	"bytes"
	"fmt"
	"net"
	"time"

	"github.com/lthiede/cartero/consume"
	"github.com/lthiede/cartero/partition"
	"github.com/lthiede/cartero/partitionmanager"
	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

const timeout time.Duration = 60 * time.Second

type Connection struct {
	conn               net.Conn
	partitionCache     map[string]*partition.Partition
	partitionManager   *partitionmanager.PartitionManager
	partitionConsumers map[string]*consume.PartitionConsumer
	responses          chan *pb.Response
	logger             *zap.Logger
	acks               chan *pb.ProduceAck
	quit               chan struct{}
}

func New(conn net.Conn, partitionManager *partitionmanager.PartitionManager, logger *zap.Logger) (*Connection, error) {
	c := &Connection{
		conn:               conn,
		partitionCache:     make(map[string]*partition.Partition),
		partitionManager:   partitionManager,
		partitionConsumers: make(map[string]*consume.PartitionConsumer),
		responses:          make(chan *pb.Response),
		logger:             logger,
		acks:               make(chan *pb.ProduceAck),
		quit:               make(chan struct{}),
	}
	go c.handleRequests()
	go c.handleResponses()
	return c, nil
}

func (c *Connection) handleRequests() {
	for {
		select {
		case <-c.quit:
			c.logger.Info("Stop handling requests")
			return
		default:
			c.conn.SetDeadline(time.Now().Add(timeout))
			request := &pb.Request{}
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Conn: c.conn}, request)
			if err != nil {
				c.logger.Error("Failed to unmarshal message", zap.Error(err))
				return
			}
			switch req := request.Request.(type) {
			case *pb.Request_ProduceRequest:
				produceReq := req.ProduceRequest
				p, err := c.partition(produceReq.PartitionName)
				if err != nil {
					c.logger.Error("Error getting partition", zap.Error(err))
					continue
				}
				c.logger.Info("Produce request", zap.String("partitionName", produceReq.PartitionName), zap.Uint64("batchId", produceReq.BatchId))
				p.ProduceRequests <- partition.ProduceRequest{
					BatchId:         produceReq.BatchId,
					Messages:        produceReq.Messages,
					ProduceResponse: c.responses,
				}
			case *pb.Request_PingPongRequest:
				pingPongRequest := req.PingPongRequest
				p, err := c.partition(pingPongRequest.PartitionName)
				if err != nil {
					c.logger.Error("Error getting partition", zap.Error(err))
					continue
				}
				p.PingPongRequests <- partition.PingPongRequest{
					PingPongResponse: c.responses,
				}
			case *pb.Request_ConsumeRequest:
				consumeReq := req.ConsumeRequest
				p, err := c.partition(consumeReq.PartitionName)
				if err != nil {
					c.logger.Error("Error getting partition", zap.Error(err))
					continue
				}
				pc, ok := c.partitionConsumers[consumeReq.PartitionName]
				if !ok {
					newPc, err := consume.NewPartitionConsumer(p, c.SendConsumeResponse, consumeReq.StartOffset, int(consumeReq.MinNumMessages), c.logger)
					if err != nil {
						c.logger.Error("Failed to register consumer", zap.String("partitionName", consumeReq.PartitionName))
						continue
					}
					c.partitionConsumers[consumeReq.PartitionName] = newPc
					c.logger.Info("Registered partition consumer", zap.String("partitionName", consumeReq.PartitionName))
				} else {
					pc.UpdateConsumption(consumeReq.StartOffset, int(consumeReq.MinNumMessages))
				}
			case *pb.Request_CreatePartitionRequest:
				createPartitionRequest := req.CreatePartitionRequest
				c.partitionManager.CreatePartitionRequests <- partitionmanager.CreatePartitionRequest{
					PartitionName: createPartitionRequest.PartitionName,
					SendResponse:  c.SendResponse,
				}
			default:
				c.logger.Error("Request type not recognized")
			}
		}
	}
}

func (c *Connection) partition(partitionName string) (*partition.Partition, error) {
	p, ok := c.partitionCache[partitionName]
	if !ok {
		pChan := make(chan *partition.Partition)
		c.partitionManager.GetPartitionRequests <- partitionmanager.GetPartitionRequest{
			PartitionName: partitionName,
			Partition:     pChan,
		}
		p, ok = <-pChan
		if !ok {
			return nil, fmt.Errorf("partition %s not recognized ", partitionName)
		}
		c.logger.Info("Added partition to connection partition cache", zap.String("partitionName", partitionName))
		c.partitionCache[partitionName] = p
	}
	return p, nil
}

func (c *Connection) SendResponse(res *pb.CreatePartitionResponse) {
	response := &pb.Response{
		Response: &pb.Response_CreatePartitionResponse{
			CreatePartitionResponse: res,
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, response)
	if err != nil {
		c.logger.Error("Failed to marshal create partition response",
			zap.Error(err))
		return
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		c.logger.Error("Failed to send create partition response",
			zap.Error(err))
		return
	}
}

func (c *Connection) handleResponses() {
	for {
		select {
		case <-c.quit:
			return
		case response := <-c.responses:
			wireMessage := &bytes.Buffer{}
			_, err := protodelim.MarshalTo(wireMessage, response)
			if err != nil {
				c.logger.Error("Failed to marshal response",
					zap.Error(err))
			}
			_, err = c.conn.Write(wireMessage.Bytes())
			if err != nil {
				c.logger.Error("Failed to send response",
					zap.Error(err))
			}
		}
	}

}

func (c *Connection) SendConsumeResponse(res *pb.ConsumeResponse) {
	response := &pb.Response{
		Response: &pb.Response_ConsumeResponse{
			ConsumeResponse: res,
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, response)
	if err != nil {
		c.logger.Error("Failed to marshal safe offset response",
			zap.Error(err),
			zap.Int("safeEndOffset", int(res.EndOfSafeOffsetsExclusively)),
			zap.String("partitionName", res.PartitionName))
		return
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		c.logger.Error("Failed to send safe offset to client",
			zap.Error(err),
			zap.Int("safeEndOffset", int(res.EndOfSafeOffsetsExclusively)),
			zap.String("partitionName", res.PartitionName))
		return
	}
}

func (c *Connection) Close() error {
	c.logger.Debug("Closing connection")
	close(c.quit)
	for _, pc := range c.partitionConsumers {
		return pc.Close()
	}
	return nil
}
