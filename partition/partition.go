package partition

import (
	"sync"
	"sync/atomic"

	pb "github.com/lthiede/cartero/proto"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

type Partition struct {
	Name                string
	Alive               bool
	AliveLock           sync.RWMutex
	ProduceRequests     chan ProduceRequest
	PingPongRequests    chan PingPongRequest
	objectStorageClient *minio.Client
	quit                chan int
	logger              *zap.Logger
	pingPongCount       atomic.Uint64 // initialized to default value 0
	nextProduceOffset   atomic.Uint64 // initialized to default value 0
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
	// bucketExists, err := objectStorageClient.BucketExists(context.Background(), name)
	// if err != nil {
	// 	return nil, fmt.Errorf("error checking if bucket already exists: %v", err)
	// }
	// if bucketExists {
	// 	logger.Warn("Bucket already exists. It might contain old data", zap.String("partitionName", name))
	// } else {
	// 	err := objectStorageClient.MakeBucket(context.Background(), name, minio.MakeBucketOptions{})
	// 	if err != nil {
	// 		return nil, fmt.Errorf("error creating bucket: %v", err)
	// 	}
	// 	logger.Debug("Created bucket", zap.String("partitionName", name))
	// }
	p := &Partition{
		Name:                name,
		Alive:               true,
		ProduceRequests:     make(chan ProduceRequest),
		PingPongRequests:    make(chan PingPongRequest),
		objectStorageClient: objectStorageClient,
		quit:                make(chan int),
		logger:              logger,
	}
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
		// _ would contain the number of bytes written
		// _, err := protodelim.MarshalTo(p.storage, pr.Messages)
		// if err != nil {
		// 	p.logger.Error("Failed to write batch to file", zap.Error(err))
		// 	p.Close()
		// 	continue
		// }
		numberMessages := len(pr.Messages.Messages)
		p.logger.Info("Successfully persisted batch", zap.String("partitionName", p.Name), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", numberMessages))
		oldNextProduceOffset := p.nextProduceOffset.Load()
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
		p.nextProduceOffset.Store(oldNextProduceOffset + uint64(numberMessages))
	}
}

func (p *Partition) NextProduceOffset() uint64 {
	return p.nextProduceOffset.Load()
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
	//p.logger.Debug("Closing file", zap.String("partitionName", p.Name), zap.String("file", p.storage.Name()))
	//p.storage.Close()
	return nil
}
