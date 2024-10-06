package partition

import (
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/lthiede/cartero/proto"

	logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

type Partition struct {
	Name              string
	Alive             bool
	AliveLock         sync.RWMutex
	ProduceRequests   chan ProduceRequest
	PingPongRequests  chan PingPongRequest
	quit              chan int
	logger            *zap.Logger
	logMock           *logMock
	pingPongCount     atomic.Uint64 // initialized to default value 0
	nextProduceOffset atomic.Uint64 // initialized to default value 0
}

type ProduceRequest struct {
	BatchId         uint64
	Messages        *pb.Messages
	ProduceResponse chan *pb.Response
}

type PingPongRequest struct {
	PingPongResponse chan *pb.Response
}

type ConsumeRequest struct {
	Quit   chan struct{}
	Notify chan uint64
}

func New(name string, objectStorageClient *minio.Client, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partitionName", name))
	logMock, err := NewLogMock(objectStorageClient, name, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating mock of log: %v", err)
	}
	p := &Partition{
		Name:             name,
		Alive:            true,
		ProduceRequests:  make(chan ProduceRequest),
		PingPongRequests: make(chan PingPongRequest),
		logMock:          logMock,
		quit:             make(chan int),
		logger:           logger,
	}
	logclient.New([]string{"127.0.0.1:50000"}, logclient.MaxOutstanding, logclient.UringEntries, logclient.UringFlagNoSingleIssuer)
	go p.handleProduce()
	go p.handlePingPong()
	return p, nil
}

func (p *Partition) handleProduce() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	for {
		pr, ok := <-p.ProduceRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
		p.logMock.Persist(pr.Messages)
		numberMessages := len(pr.Messages.Messages)
		oldNextProduceOffset := p.nextProduceOffset.Load()
		p.nextProduceOffset.Store(oldNextProduceOffset + uint64(numberMessages))
		p.logger.Info("Successfully persisted batch", zap.String("partitionName", p.Name), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", numberMessages))
		p.logger.Info("Acknowledging batch",
			zap.Uint64("batchId", pr.BatchId),
			zap.String("partitionName", p.Name),
			zap.Uint64("startOffset", oldNextProduceOffset),
			zap.Uint64("endOffset", oldNextProduceOffset+uint64(numberMessages)))
		pr.ProduceResponse <- &pb.Response{
			Response: &pb.Response_ProduceAck{
				ProduceAck: &pb.ProduceAck{
					BatchId:       pr.BatchId,
					PartitionName: p.Name,
					StartOffset:   oldNextProduceOffset,
					EndOffset:     oldNextProduceOffset + uint64(numberMessages),
				},
			},
		}
	}
}

func (p *Partition) NextProduceOffset() uint64 {
	// return p.nextProduceOffset.Load()
	// temporarily we return the nextProduceOffset in minio
	return p.logMock.startOffsetCurrentFile
}

func (p *Partition) S3ObjectNames(startOffset uint64, endOffsetExclusively uint64) []string {
	response := make(chan []string)
	p.logMock.s3ObjectNameRequests <- s3ObjectNameRequest{
		startOffset:          startOffset,
		endOffsetExclusively: endOffsetExclusively,
		response:             response,
	}
	return <-response
}

func (p *Partition) handlePingPong() {
	p.logger.Info("Start handling ping pong", zap.String("partitionName", p.Name))
	for {
		ppr, ok := <-p.PingPongRequests
		if !ok {
			p.logger.Info("Stop handling ping pong", zap.String("partitionName", p.Name))
			return
		}
		oldPingPongCount := p.pingPongCount.Load()
		ppr.PingPongResponse <- &pb.Response{
			Response: &pb.Response_PingPongResponse{
				PingPongResponse: &pb.PingPongResponse{
					PartitionName: p.Name,
				},
			},
		}
		p.pingPongCount.Store(oldPingPongCount + 1)
	}
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	p.logMock.Close()
	return nil
}
