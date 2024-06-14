package partition

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Partition struct {
	Name              string
	ProduceRequests   chan ProduceRequest
	PingPongRequests  chan PingPongRequest
	storage           *os.File
	quit              chan int
	logger            *zap.Logger
	pingPongCount     atomic.Uint64 // initialized to default value 0
	nextProduceOffset atomic.Uint64 // initialized to default value 0
}

type ProduceRequest struct {
	BatchId  uint64
	Messages *pb.Messages
	SendAck  func(*pb.ProduceAck)
}

type PingPongRequest struct {
	SendPingPongResponse func(*pb.PingPongResponse)
}

type ConsumeRequest struct {
	Quit   chan struct{}
	Notify chan uint64
}

func New(name string, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partitionName", name))
	path := filepath.Join(".", "data")
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("error creating storage directory: %v", err)
	}
	file, err := os.Create(fmt.Sprintf("data/%s", name))
	if err != nil {
		return nil, fmt.Errorf("error creating the storage file: %v", err)
	}
	logger.Debug("Created file", zap.String("partitionName", name), zap.String("file", file.Name()))
	p := &Partition{
		Name:             name,
		ProduceRequests:  make(chan ProduceRequest),
		PingPongRequests: make(chan PingPongRequest),
		storage:          file,
		quit:             make(chan int),
		logger:           logger,
	}
	go p.handleProduce()
	go p.handlePingPong()
	return p, nil
}

func (p *Partition) handleProduce() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	for {
		select {
		case pr := <-p.ProduceRequests:
			// _ would contain the number of bytes written
			_, err := protodelim.MarshalTo(p.storage, pr.Messages)
			if err != nil {
				p.logger.Error("Failed to write batch to file", zap.Error(err))
				p.Close()
				continue
			}
			numberMessages := len(pr.Messages.Messages)
			p.logger.Info("Successfully persisted batch", zap.String("partitionName", p.Name), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", numberMessages))
			oldNextProduceOffset := p.nextProduceOffset.Load()
			pr.SendAck(&pb.ProduceAck{
				BatchId:       pr.BatchId,
				PartitionName: p.Name,
				StartOffset:   oldNextProduceOffset,
				EndOffset:     oldNextProduceOffset + uint64(numberMessages),
			})
			p.nextProduceOffset.Store(oldNextProduceOffset + uint64(numberMessages))
		case <-p.quit:
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
	}
}

func (p *Partition) NextProduceOffset() uint64 {
	return p.nextProduceOffset.Load()
}

func (p *Partition) handlePingPong() {
	p.logger.Info("Start handling ping pong", zap.String("partitionName", p.Name))
	for {
		select {
		case ppr := <-p.PingPongRequests:
			oldPingPongCount := p.pingPongCount.Load()
			ppr.SendPingPongResponse(&pb.PingPongResponse{
				PartitionName: p.Name,
			})
			p.pingPongCount.Store(oldPingPongCount + 1)
		case <-p.quit:
			p.logger.Info("Stop handling ping pong", zap.String("partitionName", p.Name))
			return
		}
	}
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	p.logger.Debug("Closing file", zap.String("partitionName", p.Name), zap.String("file", p.storage.Name()))
	p.storage.Close()
	return nil
}
