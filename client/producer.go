package client

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

var MaxPublishDelay = 50 * time.Millisecond
var MaxBatchSize uint32 = 524288
var MaxMessageSize = 3800

type Producer struct {
	client                *Client
	partitionName         string
	messages              [][]byte
	endOffsetsExclusively []uint32
	epoch                 int
	lock                  sync.Mutex
	batchId               uint64        // implicitly starts at 0
	batchIdUnacknowledged atomic.Uint64 // implicitly starts at 0
	lastLSNPlus1          atomic.Uint64 // implicitly starts at 0
	numMessagesAck        atomic.Uint64 // implicitly starts at 0
	Error                 chan ProducerError
	Acks                  chan uint64
	returnAcksOnChan      bool
	done                  chan struct{}
	// for measuring latencies
	measureLatencies  atomic.Bool
	waiting           atomic.Bool
	modulo            uint64
	waitingForBatchId uint64
	sendTimes         []time.Time
	ackTimes          []time.Time
}

type ProducerError struct {
	Messages [][]byte
	Err      error
}

type ProducerAck struct {
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
		partitionName:    partitionName,
		messages:         make([][]byte, 0, 137),
		Error:            make(chan ProducerError),
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

func (p *Producer) AddMessage(message []byte) error {
	if len(message) > MaxMessageSize {
		return fmt.Errorf("to many bytes in message, max number of bytes is %d", MaxMessageSize)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	var oldPayloadSize uint32
	if len(p.endOffsetsExclusively) > 0 {
		oldPayloadSize = p.endOffsetsExclusively[len(p.endOffsetsExclusively)-1]
	}
	newPayloadSize := oldPayloadSize + uint32(len(message))
	if newPayloadSize > MaxBatchSize {
		p.client.logger.Info("Max batch size reached send")
		err := p.sendBatch()
		if err != nil {
			return fmt.Errorf("failed to send batch: %v", err)
		}
		newPayloadSize = uint32(len(message))
		p.epoch++
	}
	p.messages = append(p.messages, message)
	p.endOffsetsExclusively = append(p.endOffsetsExclusively, newPayloadSize)
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
		p.client.logger.Info("Scheduled send")
		err := p.sendBatch()
		if err != nil {
			p.Error <- ProducerError{
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
	p.client.connWriteMutex.Lock()
	if p.measureLatencies.Load() && !p.waiting.Load() && p.batchId%p.modulo == 0 {
		p.waitingForBatchId = p.batchId
		p.waiting.Store(true)
		p.sendTimes = append(p.sendTimes, time.Now())
	}
	header := wireMessage.Bytes()
	_, err = p.client.conn.Write(header)
	if err != nil {
		return fmt.Errorf("failed to send produce request over wire: %v", err)
	} else {
		// p.client.logger.Info("Sent produce proto header", zap.Int("numBytes", len(header)), zap.Int("numMessages", len(p.messages)))
	}
	for _, message := range p.messages {
		_, err = p.client.conn.Write(message)
		if err != nil {
			return fmt.Errorf("Failed to send produce payload over wire: %v", err)
		} else {
			// p.client.logger.Info("Sent actual message bytes", zap.Int("numBytes", len(message)))
		}
	}
	p.client.connWriteMutex.Unlock()
	p.batchId++
	p.endOffsetsExclusively = nil
	p.messages = nil
	return nil
}

func (p *Producer) UpdateAcknowledged(ack *pb.ProduceAck) {
	expectedBatchId := p.batchIdUnacknowledged.Load()
	if expectedBatchId != ack.BatchId {
		p.Error <- ProducerError{
			Err: fmt.Errorf("Received wrong ack. expected %d, got %d", expectedBatchId, ack.BatchId),
		}
	}
	if p.measureLatencies.Load() && p.waiting.Load() && p.waitingForBatchId == p.batchId {
		p.ackTimes = append(p.ackTimes, time.Now())
		p.waiting.Store(false)
	}
	p.batchIdUnacknowledged.Store(ack.BatchId + 1)
	p.lastLSNPlus1.Store(ack.StartLsn + uint64(ack.NumMessages))
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

func (p *Producer) StartMeasuringLatencies(modulo uint64) {
	p.measureLatencies.Store(true)
	p.modulo = modulo
}

func (p *Producer) StopMeasuringLatencies() []time.Duration {
	p.measureLatencies.Store(false)
	latencies := make([]time.Duration, len(p.ackTimes))
	for i, ackTime := range p.ackTimes {
		latencies[i] = ackTime.Sub(p.sendTimes[i])
	}
	return latencies
}

func (p *Producer) Close() error {
	p.client.logger.Info("Finished produce call", zap.String("partitionName", p.partitionName))

	p.client.producersRWMutex.Lock()
	delete(p.client.producers, p.partitionName)
	p.client.producersRWMutex.Unlock()

	close(p.done)
	return nil
}
