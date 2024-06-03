package consume

import (
	"github.com/lthiede/cartero/partition"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

type update struct {
	startOffset    uint64
	minNumMessages int
}

type PartitionConsumer struct {
	p                   *partition.Partition
	sendConsumeResponse func(*pb.ConsumeResponse)
	startOffset         uint64
	nextOffset          uint64
	minNumMessages      int
	logger              *zap.Logger
	update              chan update
	notify              chan uint64
	quit                chan struct{}
}

func NewPartitionConsumer(p *partition.Partition, sendConsumeResponse func(*pb.ConsumeResponse), startOffset uint64, minNumMessages int, logger *zap.Logger) (*PartitionConsumer, error) {
	pc := &PartitionConsumer{
		p:                   p,
		sendConsumeResponse: sendConsumeResponse,
		startOffset:         startOffset,
		minNumMessages:      minNumMessages,
		logger:              logger,
		notify:              make(chan uint64),
		quit:                make(chan struct{}),
	}
	logger.Info("Adding partition consumer", zap.String("partitionName", p.Name), zap.Uint64("startOffset", startOffset))
	p.ConsumeRequests <- partition.ConsumeRequest{
		Notify: pc.notify,
		Quit:   pc.quit,
	}
	go pc.handleConsume()
	return pc, nil
}

func (pc *PartitionConsumer) UpdateConsumption(startOffset uint64, minNumMessages int) {
	pc.logger.Info("Updating partition consumer", zap.String("partitionName", pc.p.Name), zap.Uint64("startOffset", startOffset), zap.Int("minNumMessages", minNumMessages))
	pc.update <- update{
		startOffset:    startOffset,
		minNumMessages: minNumMessages,
	}
}

func (pc *PartitionConsumer) handleConsume() {
	for {
		select {
		case <-pc.quit:
			pc.logger.Info("Stop handling consume")
			return
		case newNextOffset := <-pc.notify:
			if newNextOffset < pc.startOffset {
				pc.logger.Info("Ignoring consume notification",
					zap.String("partitionName", pc.p.Name),
					zap.Uint64("newNextOffset", newNextOffset),
					zap.Uint("startOffset", uint(pc.startOffset)))
				continue
			}
			pc.logger.Info("Sending safe consume offset",
				zap.String("partitionName", pc.p.Name),
				zap.Uint64("newNextOffset", newNextOffset))
			pc.sendConsumeResponse(&pb.ConsumeResponse{
				EndOfSafeOffsetsExclusively: newNextOffset,
				PartitionName:               pc.p.Name,
			})
			pc.nextOffset = newNextOffset
		case update := <-pc.update:
			pc.startOffset = update.startOffset
			pc.minNumMessages = update.minNumMessages
		}
	}
}

func (pc *PartitionConsumer) Close() error {
	close(pc.quit)
	return nil
}
