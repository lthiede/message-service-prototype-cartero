package client

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

var MaxMessagesPerBatch = 1
var MaxPublishDelay = 50 * time.Millisecond
var MaxBatchSize = 3800

type Producer struct {
	client                *Client
	conn                  net.Conn
	logger                *zap.Logger
	partitionName         string
	messages              [][]byte
	epoch                 int
	lock                  sync.Mutex
	batchId               uint64        // implicitly starts at 0
	batchIdUnacknowledged atomic.Uint64 // implicitly starts at 0
	lastLSNPlus1          atomic.Uint64 // implicitly starts at 0
	numMessagesAck        atomic.Uint64 // implicitly starts at 0
	Error                 chan ProduceError
	Acks                  chan uint64
	returnAcksOnChan      bool
	done                  chan struct{}
}

type ProduceError struct {
	Messages [][]byte
	Err      error
}

type ProduceAck struct {
	BatchId        uint64
	NumMessagesAck uint64
}

func (client *Client) NewProducer(partitionName string, ReturnAcksOnChan bool) (*Producer, error) {
	client.producersRWMutex.Lock()
	p, ok := client.producers[partitionName]
	if ok {
		return p, nil
	}
	p = &Producer{
		client:           client,
		conn:             client.Conn,
		logger:           client.logger,
		partitionName:    partitionName,
		Error:            make(chan ProduceError),
		Acks:             make(chan uint64),
		returnAcksOnChan: ReturnAcksOnChan,
		done:             make(chan struct{}),
	}
	if !ReturnAcksOnChan {
		close(p.Acks)
	}
	client.producers[partitionName] = p
	client.producersRWMutex.Unlock()
	return p, nil
}

func (p *Producer) AddBatch(message []byte) error {
	if p.onlyMessageBatchSize(message) > MaxBatchSize {
		return fmt.Errorf("to many bytes in message, max number of bytes is %d", MaxBatchSize)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.addedMessageBatchSize(message) > MaxBatchSize {
		err := p.sendBatch()
		if err != nil {
			return fmt.Errorf("failed to send batch: %v", err)
		}
		p.epoch++
	}
	p.messages = append(p.messages, message)
	if len(p.messages) == 1 && MaxMessagesPerBatch > 1 {
		p.scheduleSend(p.epoch)
	}
	if len(p.messages) < MaxMessagesPerBatch {
		return nil
	}
	err := p.sendBatch()
	if err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}
	p.epoch++
	return nil
}

func (p *Producer) scheduleSend(epoch int) {
	time.Sleep(MaxPublishDelay)
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.epoch == epoch {
		err := p.sendBatch()
		if err != nil {
			p.Error <- ProduceError{
				Messages: p.messages,
				Err:      fmt.Errorf("failed to send batch: %v", err),
			}
		}
	}
}

func (p *Producer) onlyMessageBatchSize(newMessage []byte) int {
	return proto.Size(&pb.Messages{
		Messages: [][]byte{newMessage},
	})
}

func (p *Producer) addedMessageBatchSize(newMessage []byte) int {
	return proto.Size(&pb.Messages{
		Messages: append(p.messages, newMessage),
	})
}

func (p *Producer) sendBatch() error {
	req := &pb.Request{
		Request: &pb.Request_ProduceRequest{
			ProduceRequest: &pb.ProduceRequest{
				BatchId:       p.batchId,
				PartitionName: p.partitionName,
				Messages:      &pb.Messages{Messages: p.messages},
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}
	_, err = p.conn.Write(wireMessage.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send bytes over wire: %v", err)
	}
	p.batchId++
	p.messages = nil
	return nil
}

func (p *Producer) UpdateAcknowledged(ack *pb.ProduceAck) {
	p.batchIdUnacknowledged.Store(ack.BatchId + 1)
	p.lastLSNPlus1.Store(ack.Lsn + 1)
	newNumMessagesAck := p.numMessagesAck.Load() + uint64(ack.NumMessages)
	p.numMessagesAck.Store(newNumMessagesAck)
	// this can block the main loop for receiving messages
	if p.returnAcksOnChan {
		p.Acks <- newNumMessagesAck
	}
}

func (p *Producer) NumMessagesAck() uint64 {
	return p.numMessagesAck.Load()
}

func (p *Producer) BatchIdAck() uint64 {
	return p.batchIdUnacknowledged.Load() - 1
}

func (p *Producer) LastLSNPlus1() uint64 {
	return p.lastLSNPlus1.Load()
}

func (p *Producer) Close() error {
	p.logger.Info("Finished produce call", zap.String("partitionName", p.partitionName))

	p.client.producersRWMutex.Lock()
	delete(p.client.producers, p.partitionName)
	p.client.producersRWMutex.Unlock()

	close(p.done)
	return nil
}
