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
)

var MaxPublishDelay = 50 * time.Millisecond
var MaxBatchSize = 524288
var MaxMessageSize = 3800

type Producer struct {
	client                *Client
	conn                  net.Conn
	connWriteMutex        *sync.Mutex
	logger                *zap.Logger
	partitionName         string
	messages              [][]byte
	endOffsetsExclusively []uint32
	payloadSize           uint32
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
		connWriteMutex:   &client.connWriteMutex,
		logger:           client.logger,
		partitionName:    partitionName,
		messages:         make([][]byte, 0, 137),
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
	if len(message) > MaxMessageSize {
		return fmt.Errorf("to many bytes in message, max number of bytes is %d", MaxMessageSize)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if int(p.payloadSize)+len(message) > MaxBatchSize {
		err := p.sendBatch()
		if err != nil {
			return fmt.Errorf("failed to send batch: %v", err)
		}
		p.epoch++
	}
	p.messages = append(p.messages, message)
	p.payloadSize += uint32(len(message))
	p.endOffsetsExclusively = append(p.endOffsetsExclusively, p.payloadSize)
	if len(p.messages) == 1 {
		go p.scheduleSend(p.epoch)
	}
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

func (p *Producer) sendBatch() error {
	req := &pb.Request{
		Request: &pb.Request_ProduceRequest{
			ProduceRequest: &pb.ProduceRequest{
				BatchId:               p.batchId,
				PartitionName:         p.partitionName,
				EndOffsetsExclusively: p.endOffsetsExclusively,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}
	p.connWriteMutex.Lock()
	_, err = p.conn.Write(wireMessage.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send produce request over wire: %v", err)
	}
	for _, message := range p.messages {
		_, err = p.conn.Write(message)
		if err != nil {
			return fmt.Errorf("Failed to send produce payload over wire: %v", err)
		}
	}
	p.connWriteMutex.Unlock()
	p.batchId++
	p.messages = nil
	p.payloadSize = 0
	return nil
}

func (p *Producer) UpdateAcknowledged(ack *pb.ProduceAck) {
	p.batchIdUnacknowledged.Store(ack.BatchId + 1)
	p.lastLSNPlus1.Store(ack.StartLsn + uint64(ack.NumMessages) + 1)
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
