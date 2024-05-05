package connection

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/lthiede/cartero/messages"
	"github.com/lthiede/cartero/partition"
	"go.uber.org/zap"
)

/*
Which requests can be handled in parallel and which have to be synchronized

Kafka:
Kafka guarantees message ordering per client partition
Kafka also guarantees that requests on a connection are processed and responses
send in the order the requests were received

This seems limiting and unnecessary.
For now I choose only to handle messages from the same client to the same partition
in strict order, but I should discuss this with tobias

Which errors are recoverable and which require the connection to be closed
*/

/*
Structure of Requests
Request Length + Request Type + Payload

Payload for Produce:
Partition + BatchId + (Message Length + Message) * n
*/

/*
Structure of Responses
Response Length + Response Type + Payload

Payload for Produce Ack:
Partition + BatchId
*/

type Connection struct {
	conn        net.Conn
	partitions  map[string]partition.Partition
	produceAcks chan messages.ProduceAck
	quit        chan int
	logger      *zap.Logger
}

const (
	RequestTypeProduce byte = iota
	RequestTypeConsume
	RequestTypeCreatePartition
)
const (
	ResponseTypeAckProduce byte = iota
)

func New(conn net.Conn, partitions map[string]partition.Partition, logger *zap.Logger) *Connection {
	c := &Connection{
		conn,
		partitions,
		make(chan messages.ProduceAck),
		make(chan int),
		logger,
	}
	return c
}

func (c *Connection) HandleRequests() {
	go c.HandleResponses()
	c.logger.Info("Start handling requests")
	for {
		select {
		case <-c.quit:
			c.logger.Info("Stop handling requests")
			return
		default:
			request, err := messages.ProtocolMessage(c.conn, c.logger)
			if err != nil {
				c.logger.Error("Error reading request", zap.Error(err))
				c.Close()
				continue
			}
			err = c.handleRequest(request)
			if err != nil {
				c.logger.Error("Error handling request", zap.Error(err))
				c.Close()
			}
		}
	}
}

func (c *Connection) handleRequest(request []byte) error {
	switch request[0] {
	case RequestTypeProduce:
		c.logger.Info("Handling produce request")
		err := c.produce(request[1:])
		if err != nil {
			return fmt.Errorf("error handling produce request %v", err)
		}
	case RequestTypeConsume:
		c.logger.Info("Handling consume request")
		err := c.consume(request[1:])
		if err != nil {
			return fmt.Errorf("error handling consume request %v", err)
		}
	case RequestTypeCreatePartition:
		c.logger.Info("Handling topic creation request")
		err := c.topic(request[1:])
		if err != nil {
			return fmt.Errorf("error handling partition request %v", err)
		}
	default:
		return fmt.Errorf("received unrecognized request %v", request[0])
	}
	return nil
}

func (c *Connection) Close() error {
	c.logger.Debug("Closing connection")
	close(c.quit)
	c.conn.Close()
	return nil
}

func (c *Connection) produce(request []byte) error {
	partitionName, bytesUsed, err := messages.NextString(request, c.logger)
	if err != nil {
		return fmt.Errorf("error parsing the partition name: %v", err)
	}
	c.logger.Debug("Parsed", zap.String("partitionName", partitionName))
	bytesUsedTotal := bytesUsed
	batchId, bytesUsed, err := messages.NextUInt64(request[bytesUsedTotal:])
	if err != nil {
		return fmt.Errorf("error parsing the batch id: %v", err)
	}
	c.logger.Debug("Parsed", zap.Uint64("batchId", batchId))
	bytesUsedTotal += bytesUsed
	p := c.partitions[partitionName]
	p.Input <- messages.ProduceRequest{
		ProduceAck: c.produceAcks,
		BatchId:    batchId,
		Payload:    request[bytesUsedTotal:],
	}
	return nil
}

func (c *Connection) consume(request []byte) error {
	// stub
	return nil
}

func (c *Connection) topic(request []byte) error {
	// stub
	return nil
}

func (c *Connection) HandleResponses() {
	c.logger.Info("Start handling responses")
	for {
		select {
		case produceAck := <-c.produceAcks:
			err := c.ackProduce(produceAck)
			if err != nil {
				c.logger.Error("Failed to acknowledge produce", zap.Error(err))
				c.Close()
			}
		case <-c.quit:
			c.logger.Info("Stop handling responses")
			return
		}
	}
}

func (c *Connection) ackProduce(ack messages.ProduceAck) error {
	// not including bytes encoding response length
	responseLen := 1 + 2 + len(ack.PartitionName) + 8
	responseLengthEncodingLen := 4
	response := make([]byte, 0, responseLen+responseLengthEncodingLen)
	response = binary.BigEndian.AppendUint32(response, uint32(responseLen))
	response = append(response, ResponseTypeAckProduce)
	response = binary.BigEndian.AppendUint16(response, uint16(len(ack.PartitionName)))
	response = append(response, []byte(ack.PartitionName)...)
	response = binary.BigEndian.AppendUint64(response, uint64(ack.BatchId))
	n, err := c.conn.Write(response)
	if err != nil {
		if n != 5 {
			return fmt.Errorf("failed to write complete acknowledge produce response: %v", err)
		}
		c.logger.Error("Error writing acknowledge produce response", zap.Error(err))
	}
	c.logger.Info("Acknowledged batch", zap.String("partition", ack.PartitionName), zap.Uint64("batchId", ack.BatchId))
	return nil
}
