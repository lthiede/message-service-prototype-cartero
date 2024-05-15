package partition

import (
	"fmt"
	"os"

	"github.com/lthiede/cartero/cache"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

// TODO: analyze where we have back pressure and where not

type Partition struct {
	Name            string
	ProduceRequests chan ProduceRequest
	storage         *os.File
	quit            chan int
	logger          *zap.Logger
	Offset          uint64
	cache           *cache.Cache
}

type ProduceRequest struct {
	BatchId  uint64
	Messages *pb.Messages
	Ack      chan *pb.ProduceAck
}

func New(name string, cache *cache.Cache, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partitionName", name))
	file, err := os.Create(fmt.Sprintf("/home/lorenz/Desktop/Master_Thesis/cartero/data/%s", name))
	if err != nil {
		return nil, fmt.Errorf("error creating the storage file: %v", err)
	}
	logger.Debug("Created file", zap.String("partitionName", name), zap.String("file", file.Name()))
	p := &Partition{
		name,
		make(chan ProduceRequest),
		file,
		make(chan int),
		logger,
		0,
		cache,
	}
	go p.handleProduce()
	return p, nil
}

func (p *Partition) handleProduce() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	for {
		select {
		case pr := <-p.ProduceRequests:
			p.logger.Info("Persisting batch", zap.String("partitionName", p.Name), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", len(pr.Messages.Messages)))
			// _ would contain the number of bytes written
			_, err := protodelim.MarshalTo(p.storage, pr.Messages)
			if err != nil {
				p.logger.Error("Failed to write batch to file", zap.Error(err))
				p.Close()
			}
			numberMessages := len(pr.Messages.Messages)
			p.logger.Info("Successfully persisted batch", zap.String("partitionName", p.Name), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", numberMessages))
			pr.Ack <- &pb.ProduceAck{
				BatchId:     pr.BatchId,
				StartOffset: uint64(p.Offset),
				EndOffset:   uint64(p.Offset + uint64(numberMessages)),
			}
			p.Offset += uint64(numberMessages)
			p.cache.Write(pr.Messages.Messages, p.Name)
		case <-p.quit:
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
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
