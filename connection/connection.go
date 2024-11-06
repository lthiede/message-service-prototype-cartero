package connection

import (
	"bytes"
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
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, request)
			if err != nil {
				c.logger.Error("Failed to unmarshal message", zap.Error(err))
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
					}
					i += n
				}
				p, ok := c.partitionCache[produceReq.PartitionName]
				if !ok {
					p, err = c.partitionManager.GetPartition(produceReq.PartitionName)
					if err != nil {
						c.logger.Error("Error getting partition", zap.Error(err))
						continue
					}
					c.partitionCache[produceReq.PartitionName] = p
				}
				p.AliveLock.RLock()
				if !p.Alive {
					delete(c.partitionCache, produceReq.PartitionName)
					c.logger.Error("Produce request to dead partition", zap.String("partitionName", produceReq.PartitionName), zap.Uint64("batchId", produceReq.BatchId))
				} else {
					p.LogInteractionRequests <- partition.LogInteractionRequest{
						AppendBatchRequest: &partition.AppendBatchRequest{
							BatchId:               produceReq.BatchId,
							EndOffsetsExclusively: produceReq.EndOffsetsExclusively,
							Payload:               payload,
							ProduceResponse:       c.responses,
						},
					}
				}
				p.AliveLock.RUnlock()
			case *pb.Request_ConsumeRequest:
				consumeReq := req.ConsumeRequest
				p, ok := c.partitionCache[consumeReq.PartitionName]
				if !ok {
					p, err = c.partitionManager.GetPartition(consumeReq.PartitionName)
					if err != nil {
						c.logger.Error("Error getting partition", zap.Error(err))
						continue
					}
				}
				pc, ok := c.partitionConsumers[consumeReq.PartitionName]
				if !ok {
					newPc, err := consume.NewPartitionConsumer(p, c.SendResponse, consumeReq.StartLsn, int(consumeReq.MinNumMessages), c.logger)
					if err != nil {
						c.logger.Error("Failed to register consumer", zap.String("partitionName", consumeReq.PartitionName))
						continue
					}
					c.partitionConsumers[consumeReq.PartitionName] = newPc
					c.logger.Info("Registered partition consumer", zap.String("partitionName", consumeReq.PartitionName))
				} else {
					pc.UpdateConsumption(consumeReq.StartLsn, int(consumeReq.MinNumMessages))
				}
			case *pb.Request_CreatePartitionRequest:
				createPartitionRequest := req.CreatePartitionRequest
				err := c.partitionManager.CreatePartition(createPartitionRequest.PartitionName)
				if err != nil {
					c.logger.Error("Failed to create partition", zap.Error(err))
				}
				response := &pb.Response{
					Response: &pb.Response_CreatePartitionResponse{
						CreatePartitionResponse: &pb.CreatePartitionResponse{
							PartitionName: createPartitionRequest.PartitionName,
							Successful:    err == nil,
						},
					},
				}
				c.SendResponse(response)
			default:
				c.logger.Error("Request type not recognized")
			}
		}
	}
}

func (c *Connection) SendResponse(res *pb.Response) {
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, res)
	if err != nil {
		c.logger.Error("Failed to marshal response",
			zap.Error(err))
		return
	}
	_, err = c.conn.Write(wireMessage.Bytes())
	if err != nil {
		c.logger.Error("Failed to send response",
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
			c.SendResponse(response)
		}
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
