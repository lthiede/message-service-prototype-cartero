package partition

import (
	"fmt"
	"os"

	"github.com/lthiede/cartero/messages"
	"go.uber.org/zap"
)

type Partition struct {
	Name    string
	Input   chan messages.ProduceRequest
	storage *os.File
	quit    chan int
	logger  *zap.Logger
}

func New(name string, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partition", name))
	file, err := os.Create(fmt.Sprintf("data/%s", name))
	if err != nil {
		return nil, fmt.Errorf("error creating the storage file: %v", err)
	}
	logger.Debug("Created file", zap.String("partition", name), zap.String("file", file.Name()))
	return &Partition{
		name,
		make(chan messages.ProduceRequest),
		file,
		make(chan int),
		logger,
	}, nil
}

func (p *Partition) HandleProduce() {
	p.logger.Info("Start handling produce", zap.String("partition", p.Name))
	for {
		select {
		case pr := <-p.Input:
			p.logger.Info("Persisting batch", zap.String("partition", p.Name), zap.Uint64("batchId", pr.BatchId))
			n, err := p.storage.Write(pr.Payload)
			if err != nil {
				p.logger.Error("Failed to write batch to file", zap.Int("numberBytesWritten", n), zap.Int("numberBytesTotal", len(pr.Payload)), zap.Error(err))
				p.Close()
			}
			p.logger.Info("Successfully persisted batch", zap.String("partition", p.Name), zap.Uint64("batchId", pr.BatchId))
			pr.ProduceAck <- messages.ProduceAck{
				BatchId:       pr.BatchId,
				PartitionName: p.Name,
			}
		case <-p.quit:
			p.logger.Info("Stop handling produce", zap.String("partition", p.Name))
			return
		}
	}
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partition", p.Name))
	close(p.quit)
	p.logger.Debug("Closing file", zap.String("partition", p.Name), zap.String("file", p.storage.Name()))
	p.storage.Close()
	return nil
}
