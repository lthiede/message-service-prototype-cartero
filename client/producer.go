package client

import (
	"bytes"
	"net"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

const MaxMessagesPerBatch = 10
const MaxPublishDelay = 100 * time.Millisecond

type Producer struct {
	client                *Client
	conn                  net.Conn
	logger                *zap.Logger
	partitionName         string
	messages              [][]byte
	batchId               uint64        // implicitly starts at 0
	batchIdUnacknowledged atomic.Uint64 // implicitly starts at 0
	numMessagesAck        atomic.Uint64 // implicitly starts at 0
	Input                 chan []byte
	Error                 chan ProduceError
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

func (client *Client) NewProducer(partitionName string) (*Producer, error) {
	client.producersRWMutex.Lock()
	p, ok := client.producers[partitionName]
	if ok {
		return p, nil
	}
	p = &Producer{
		client:        client,
		conn:          client.conn,
		logger:        client.logger,
		partitionName: partitionName,
		Input:         make(chan []byte),
		Error:         make(chan ProduceError),
		done:          make(chan struct{}),
	}
	client.producers[partitionName] = p
	client.producersRWMutex.Unlock()
	go p.handleInput()
	return p, nil
}

func (p *Producer) handleInput() {
	p.logger.Info("Start handling production of batches", zap.String("partitionName", p.partitionName))
	maxPublishDelayEpoch := 0
	maxPublishDelayTimer := make(chan int)
	for {
		select {
		case message := <-p.Input:
			p.messages = append(p.messages, message)
			if len(p.messages) == 1 && MaxMessagesPerBatch > 1 {
				go func(epoch int) {
					time.Sleep(MaxPublishDelay)
					maxPublishDelayTimer <- epoch
				}(maxPublishDelayEpoch)
			}
			if len(p.messages) < MaxMessagesPerBatch {
				continue
			}
			p.logger.Info("MaxMessagesPerBatch reached", zap.String("partitionName", p.partitionName), zap.Int("MaxMessagesPerBatch", MaxMessagesPerBatch))
			p.sendBatch()
			p.messages = nil
			maxPublishDelayEpoch++
		case maxPublishDelayReached := <-maxPublishDelayTimer:
			if maxPublishDelayReached < maxPublishDelayEpoch {
				continue
			}
			p.logger.Info("MaxPublishDelay reached",
				zap.String("partitionName", p.partitionName),
				zap.Int("MaxPublishDelay",
					int(MaxPublishDelay.Milliseconds())))
			p.sendBatch()
			p.messages = nil
		case <-p.done:
			p.logger.Info("Stop handling production of batches", zap.String("partitionName", p.partitionName))
			return
		}
	}
}

func (p *Producer) sendBatch() {
	req := &pb.Request{
		Request: &pb.Request_ProduceRequest{
			ProduceRequest: &pb.ProduceRequest{
				BatchId:       p.batchId,
				PartitionName: p.partitionName,
				Messages:      &pb.Messages{Messages: p.messages},
			},
		},
	}
	p.logger.Info("Sending batch", zap.Uint64("batchId", p.batchId), zap.String("partitionName", p.partitionName))
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		p.logger.Error("Failed to marshal batch", zap.Uint64("batchId", p.batchId), zap.String("partitionName", p.partitionName), zap.Error(err))
		p.Error <- ProduceError{
			Err:      err,
			Messages: p.messages,
		}
		return
	}
	_, err = p.conn.Write(wireMessage.Bytes())
	if err != nil {
		p.logger.Error("Failed to send batch", zap.Uint64("batchId", p.batchId), zap.String("partitionName", p.partitionName), zap.Error(err))
		p.Error <- ProduceError{
			Err:      err,
			Messages: p.messages,
		}
		return
	}
	p.batchId++
}

func (p *Producer) UpdateAcknowledged(batchId uint64, numMessages uint64) {
	p.batchIdUnacknowledged.Store(batchId + 1)
	p.numMessagesAck.Store(p.numMessagesAck.Load() + numMessages)
}

func (p *Producer) NumMessagesAck() uint64 {
	return p.numMessagesAck.Load()
}

func (p *Producer) BatchIdAck() uint64 {
	return p.batchIdUnacknowledged.Load() - 1
}

func (p *Producer) Close() error {
	p.logger.Info("Finished produce call", zap.String("partitionName", p.partitionName))

	p.client.producersRWMutex.Lock()
	delete(p.client.producers, p.partitionName)
	p.client.producersRWMutex.Unlock()

	close(p.done)
	return nil
}
