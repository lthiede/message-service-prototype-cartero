package client

import (
	"context"
	"io"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

const MaxMessagesPerBatch = 10
const MaxPublishDelay = 100 * time.Millisecond

type Producer struct {
	cancel            context.CancelFunc
	context           context.Context
	logger            *zap.Logger
	stream            pb.Broker_ProduceClient
	partitionName     string
	messages          [][]byte
	batchId           int // implicitly starts at 0
	Input             chan []byte
	Error             chan ProduceError
	Ack               chan ProduceAck
	batchAck          *pb.ProduceAck
	ackReturnedToUser uint64
}

type ProduceError struct {
	Messages [][]byte
	Err      error
}

type ProduceAck struct {
	Offset uint64
}

func (client *Client) NewProducer(partitionName string) (*Producer, error) {
	context, cancel := context.WithCancel(context.Background())
	md := metadata.New(map[string]string{server.PartitionNameMetadataKey: partitionName})
	stream, err := client.grpcClient.Produce(metadata.NewOutgoingContext(context, md))
	if err != nil {
		client.logger.Error("Failed to issue produce", zap.String("partitionName", partitionName), zap.Error(err))
		cancel()
		return nil, err
	}
	client.logger.Info("Issued produce call", zap.String("partitionName", partitionName))
	p := &Producer{
		cancel:        cancel,
		context:       context,
		logger:        client.logger,
		stream:        stream,
		partitionName: partitionName,
		Input:         make(chan []byte),
		Error:         make(chan ProduceError),
		Ack:           make(chan ProduceAck),
	}
	go p.handleInput()
	go p.handleAcks()
	return p, nil
}

func (p *Producer) handleInput() {
	// TODO: try what happens if server side closes the call e.g. automatically after 3 messages
	p.logger.Info("Start handling production of batches", zap.String("partitionName", p.partitionName))
	done := p.context.Done()
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
			p.sendBatch(p.messages)
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
			p.sendBatch(p.messages)
			p.messages = nil
		case <-done:
			p.logger.Info("Context has been canceled. Stop handling production of batches", zap.String("partitionName", p.partitionName))
			return
		}
	}
}

func (p *Producer) handleAcks() {
	p.logger.Info("Start handling acknowledges of batches", zap.String("partitionName", p.partitionName))
	done := p.context.Done()
	for {
		select {
		case <-done:
			p.logger.Info("Context has been canceled. Stop handling acknowledge of produced batches", zap.String("partitionName", p.partitionName))
			return
		default:
			p.ackReturnedToUser++
			if p.batchAck != nil && p.ackReturnedToUser < p.batchAck.EndOffset {
				p.logger.Info("Acknowledge production of message", zap.String("partitionName", p.partitionName), zap.Int("offset", int(p.ackReturnedToUser)))
				p.Ack <- ProduceAck{p.ackReturnedToUser}
				continue
			}
			in, err := p.stream.Recv()
			if err == io.EOF {
				p.logger.Info("Stream closed", zap.String("partitionName", p.partitionName))
				return
			}
			if err != nil {
				p.logger.Error("Failed to ack", zap.String("partitionName", p.partitionName), zap.Error(err))
				continue
			}
			p.batchAck = in
			p.ackReturnedToUser = in.StartOffset
			p.Ack <- ProduceAck{p.ackReturnedToUser}
			p.logger.Info("Acknowledged produce", zap.String("partitionName", p.partitionName), zap.Int("offset", int(p.ackReturnedToUser)))
		}
	}
}

func (p *Producer) sendBatch(messages [][]byte) {
	err := p.stream.Send(&pb.ProduceMessageBatch{
		BatchId:  uint64(p.batchId),
		Messages: &pb.Messages{Messages: messages},
	})
	if err == io.EOF {
		p.logger.Error("Failed to send batch due to server side", zap.Int("batchId", p.batchId), zap.String("partitionName", p.partitionName), zap.Error(err))
		p.Error <- ProduceError{
			Err:      io.EOF,
			Messages: messages,
		}
	}
	if err != nil {
		p.logger.Error("Failed to send batch", zap.Int("batchId", p.batchId), zap.String("partitionName", p.partitionName), zap.Error(err))
		p.Error <- ProduceError{
			Err:      err,
			Messages: messages,
		}
	}
	p.logger.Info("Send batch", zap.Int("batchId", p.batchId), zap.String("partitionName", p.partitionName))
	p.batchId++
}

func (p *Producer) Close() error {
	p.logger.Info("Finished produce call", zap.String("partitionName", p.partitionName))
	close(p.Ack)
	close(p.Error)
	p.cancel()
	return nil
}
