package client

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

var MaxMessageSize = 3800

type Producer struct {
	client        *Client
	partitionName string
	// the stuff to send
	messages              [][]byte
	endOffsetsExclusively []uint32
	batchId               uint64 // implicitly starts at 0
	// for configuration from outside
	MaxPublishDelay time.Duration
	MaxBatchSize    uint32
	maxOutstanding  uint32
	// for sends due to max publish delay reached
	epoch int
	lock  sync.Mutex
	dead  bool
	// Keeping track of sent, outstanding and acks
	lastLSNPlus1       atomic.Uint64
	numMessagesSent    atomic.Uint64
	numMessagesAck     atomic.Uint64 // doesn't include lost messages or lost acks
	numBatchesHandled  atomic.Uint64 // includes lost messages or lost acks
	outstandingBatches chan Batch
	// used by acks and sends due to max publish delay reached
	AsyncError chan ProducerError
	// for measuring latencies
	measureLatencies  atomic.Bool
	waiting           atomic.Bool
	waitingForBatchId uint64
	sendTimes         []latencyMeasurement
	ackTimes          []latencyMeasurement
}

type latencyMeasurement struct {
	batchId uint64
	t       time.Time
}

type ProducerError struct {
	Batch *Batch
	Err   error
}

type Batch struct {
	Messages [][]byte
	BatchId  uint64
}

type ProducerAck struct {
	BatchId        uint64
	NumMessagesAck uint64
}

func (client *Client) NewProducer(partitionName string, maxOutstanding uint32) (*Producer, error) {
	client.producersRWMutex.Lock()
	p, ok := client.producers[partitionName]
	if ok {
		return p, nil
	}
	p = &Producer{
		client:             client,
		MaxPublishDelay:    1 * time.Millisecond,
		MaxBatchSize:       524288,
		maxOutstanding:     maxOutstanding,
		outstandingBatches: make(chan Batch, maxOutstanding),
		partitionName:      partitionName,
		messages:           make([][]byte, 0, 137),
		AsyncError:         make(chan ProducerError),
	}
	client.producers[partitionName] = p
	client.producersRWMutex.Unlock()
	return p, nil
}

func (p *Producer) AddMessage(message []byte) error {
	if len(message) > MaxMessageSize {
		return fmt.Errorf("to many bytes in message, max number of bytes is %d", MaxMessageSize)
	} else if p.MaxBatchSize == 0 {
		return p.sendSingleMessage(message)
	} else {
		return p.addMessageToBatch(message)
	}
}

func (p *Producer) sendSingleMessage(message []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.dead {
		return errors.New("Send on dead producer")
	}
	if len(p.endOffsetsExclusively) != 0 || len(p.messages) != 0 {
		return errors.New("turned of batching, while there is still a waiting batch")
	}
	p.messages = [][]byte{message}
	p.endOffsetsExclusively = []uint32{uint32(len(message))}
	err := p.sendBatch()
	if err != nil {
		return fmt.Errorf("failed to send single message: %v", err)
	}
	return nil
}

func (p *Producer) addMessageToBatch(message []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.dead {
		return errors.New("Send on dead producer")
	}
	var oldPayloadSize uint32
	if len(p.endOffsetsExclusively) > 0 {
		oldPayloadSize = p.endOffsetsExclusively[len(p.endOffsetsExclusively)-1]
	}
	newPayloadSize := oldPayloadSize + uint32(len(message))
	if newPayloadSize > p.MaxBatchSize {
		// p.client.logger.Info("Max batch size reached send")
		err := p.sendBatch()
		if err != nil {
			return fmt.Errorf("failed to send full batch: %v", err)
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
	if p.MaxPublishDelay == 0 {
		return
	}
	time.Sleep(p.MaxPublishDelay)
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.epoch == epoch {
		// p.client.logger.Info("Scheduled send")
		// len equals 0 shouldn't happen because then the epoch should be increased
		if !p.dead && len(p.messages) != 0 {
			err := p.sendBatch()
			if err != nil {
				p.AsyncError <- ProducerError{
					Batch: nil,
					Err:   fmt.Errorf("failed to asynchronously send batch: %v. batch is kept in producer", err),
				}
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
	header := wireMessage.Bytes()
	var timeWaited time.Duration
	for p.batchId >= p.numBatchesHandled.Load()+uint64(p.maxOutstanding) {
		time.Sleep(100 * time.Microsecond)
		timeWaited += 100 * time.Microsecond
		if timeWaited >= time.Second {
			// assume all outstanding batches have been lost and will never get acked
			break
		}
	}
	sentSuccessfully := false
	for !sentSuccessfully {
		p.client.epochMutex.RLock()
		potentialFailureEpoch := p.client.epoch
		err := p.sendBytesOverNetwork(header)
		p.client.epochMutex.RUnlock()
		if err != nil {
			p.client.logger.Error("Failed to send produce request or payload", zap.Error(err))
			err := p.client.restoreConnection(potentialFailureEpoch)
			if err != nil {
				return fmt.Errorf("failed to recover from network failure: %v", err)
			}
		} else {
			sentSuccessfully = true
		}
	}
	p.outstandingBatches <- Batch{
		Messages: p.messages,
		BatchId:  p.batchId,
	}
	p.numMessagesSent.Add(uint64(len(p.messages)))
	p.batchId++
	p.endOffsetsExclusively = nil
	p.messages = nil
	return nil
}

func (p *Producer) sendBytesOverNetwork(header []byte) error {
	p.client.connWriteMutex.Lock()
	defer p.client.connWriteMutex.Unlock()
	if p.measureLatencies.Load() && !p.waiting.Load() {
		p.waitingForBatchId = p.batchId
		p.waiting.Store(true)
		p.sendTimes = append(p.sendTimes, latencyMeasurement{
			batchId: p.batchId,
			t:       time.Now(),
		})
	}
	_, err := p.client.conn.Write(header)
	if err != nil {
		return fmt.Errorf("failed to send produce request over wire: %v", err)
	}
	for _, message := range p.messages {
		_, err = p.client.conn.Write(message)
		if err != nil {
			return fmt.Errorf("failed to send produce payload over wire: %v", err)
		}
	}
	return nil
}

func (p *Producer) UpdateAcknowledged(ack *pb.ProduceAck) {
	expectedBatch := <-p.outstandingBatches
	numBatchesHandled := 1
	for expectedBatch.BatchId < ack.BatchId {
		p.AsyncError <- ProducerError{
			Batch: &expectedBatch,
			Err:   fmt.Errorf("Received wrong ack. expected %d, got %d. Probably lost the messages in between", expectedBatch.BatchId, ack.BatchId),
		}
		expectedBatch = <-p.outstandingBatches
		numBatchesHandled++
	}
	p.numBatchesHandled.Add(uint64(numBatchesHandled))
	if p.measureLatencies.Load() && p.waiting.Load() {
		if ack.BatchId == p.waitingForBatchId {
			p.ackTimes = append(p.ackTimes, latencyMeasurement{
				batchId: ack.BatchId,
				t:       time.Now(),
			})
			p.waiting.Store(false)
		} else if ack.BatchId > p.waitingForBatchId {
			// lost the message we're waiting for
			p.waiting.Store(false)
		}
	}
	p.lastLSNPlus1.Store(ack.StartLsn + uint64(ack.NumMessages))
	p.numMessagesAck.Add(uint64(ack.NumMessages))
}

func (p *Producer) NumMessagesSent() uint64 {
	return p.numMessagesSent.Load()
}

func (p *Producer) NumMessagesAck() uint64 {
	return p.numMessagesAck.Load()
}

func (p *Producer) LastLSNPlus1() uint64 {
	return p.lastLSNPlus1.Load()
}

func (p *Producer) StartMeasuringLatencies() {
	p.measureLatencies.Store(true)
}

func (p *Producer) StopMeasuringLatencies() []time.Duration {
	p.measureLatencies.Store(false)
	latencies := make([]time.Duration, len(p.ackTimes))
	sendTimesIndex := 0
	for ackTimesIndex, ack := range p.ackTimes {
		send := p.sendTimes[sendTimesIndex]
		for send.batchId != ack.batchId {
			sendTimesIndex++
			send = p.sendTimes[sendTimesIndex]
		}
		latencies[ackTimesIndex] = ack.t.Sub(send.t)
	}
	return latencies
}

func (p *Producer) Close() error {
	p.client.logger.Info("Finished produce call", zap.String("partitionName", p.partitionName))

	p.client.producersRWMutex.Lock()
	delete(p.client.producers, p.partitionName)
	p.client.producersRWMutex.Unlock()

	p.lock.Lock()
	p.dead = true
	p.lock.Unlock()

	return nil
}
