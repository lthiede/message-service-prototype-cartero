package partition

import (
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Partition struct {
	Name                          string
	ProduceRequests               chan ProduceRequest
	PingPongRequests              chan PingPongRequest
	PingPongSyncLikeNotifyConsume chan struct{}
	ConsumeRequests               chan ConsumeRequest
	consumers                     []ConsumeRequest
	produceNotifyConsume          chan uint64
	storage                       *os.File
	quit                          chan int
	logger                        *zap.Logger
	NextProduceOffset             uint64
	NextConsumeOffset             uint64
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
		Name:                          name,
		ProduceRequests:               make(chan ProduceRequest),
		PingPongRequests:              make(chan PingPongRequest),
		PingPongSyncLikeNotifyConsume: make(chan struct{}),
		ConsumeRequests:               make(chan ConsumeRequest),
		produceNotifyConsume:          make(chan uint64),
		storage:                       file,
		quit:                          make(chan int),
		logger:                        logger,
		NextProduceOffset:             0,
	}
	go p.handleProduce()
	go p.handlePingPong()
	go p.pingPongSyncLikeNotifyConsume()
	go p.handleConsume()
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
			pr.SendAck(&pb.ProduceAck{
				BatchId:       pr.BatchId,
				PartitionName: p.Name,
				StartOffset:   uint64(p.NextProduceOffset),
				EndOffset:     uint64(p.NextProduceOffset + uint64(numberMessages)),
			})
			p.NextProduceOffset += uint64(numberMessages)
			p.produceNotifyConsume <- p.NextProduceOffset
		case <-p.quit:
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
	}
}

func (p *Partition) handlePingPong() {
	p.logger.Info("Start handling ping pong", zap.String("partitionName", p.Name))
	for {
		select {
		case ppr := <-p.PingPongRequests:
			ppr.SendPingPongResponse(&pb.PingPongResponse{
				PartitionName: p.Name,
			})
			p.produceNotifyConsume <- p.NextProduceOffset
		case <-p.quit:
			p.logger.Info("Stop handling ping pong", zap.String("partitionName", p.Name))
			return
		}
	}
}

func (p *Partition) pingPongSyncLikeNotifyConsume() {
	p.logger.Info("Start handling ping pong sync like notify consume", zap.String("partitionName", p.Name))
	for {
		select {
		case <-p.quit:
			p.logger.Info("Stop handling ping pong sync like notify consume", zap.String("partitionName", p.Name))
			return
		case <-p.PingPongSyncLikeNotifyConsume:
		}
	}
}

func (p *Partition) handleConsume() {
	p.logger.Info("Start handling consume", zap.String("partitionName", p.Name))
	for {
		select {
		case <-p.quit:
			p.logger.Info("Stop handling consume", zap.String("partitionName", p.Name))
			return
		case cr := <-p.ConsumeRequests:
			p.consumers = append(p.consumers, cr)
			cr.Notify <- p.NextConsumeOffset
		case newNextOffset := <-p.produceNotifyConsume:
			p.notifyConsumers(newNextOffset)
		}
	}
}

func (p *Partition) notifyConsumers(newNextOffset uint64) {
	i := 0 // output index
	for _, consumer := range p.consumers {
		select {
		case <-consumer.Quit:
		case consumer.Notify <- newNextOffset:
			p.consumers[i] = consumer
			i++
		}
	}
	p.NextConsumeOffset = newNextOffset
	p.consumers = p.consumers[:i]
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	p.logger.Debug("Closing file", zap.String("partitionName", p.Name), zap.String("file", p.storage.Name()))
	p.storage.Close()
	return nil
}
