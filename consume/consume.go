package consume

import (
	"time"

	"github.com/lthiede/cartero/partition"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

const checkForNewLatency = 100 * time.Millisecond

type update struct {
	startOffset    uint64
	minNumMessages int
}

type PartitionConsumer struct {
	p              *partition.Partition
	sendResponse   func(*pb.Response)
	startOffset    uint64
	nextOffset     uint64
	minNumMessages int
	logger         *zap.Logger
	update         chan update
	checkForNew    chan struct{}
	quit           chan struct{}
}

func NewPartitionConsumer(p *partition.Partition, sendResponse func(*pb.Response), startOffset uint64, minNumMessages int, logger *zap.Logger) (*PartitionConsumer, error) {
	pc := &PartitionConsumer{
		p:              p,
		sendResponse:   sendResponse,
		startOffset:    startOffset,
		minNumMessages: minNumMessages,
		logger:         logger,
		update:         make(chan update),
		checkForNew:    make(chan struct{}),
		quit:           make(chan struct{}),
	}
	logger.Info("Adding partition consumer", zap.String("partitionName", p.Name), zap.Uint64("startOffset", startOffset))
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
		go pc.waitBeforeCheckForNew()
		select {
		case <-pc.quit:
			pc.logger.Info("Stop handling consume")
			return
		case update := <-pc.update:
			pc.startOffset = update.startOffset
			pc.minNumMessages = update.minNumMessages
		case <-pc.checkForNew:
			newNextOffset := pc.p.NextProduceOffset()
			if newNextOffset < pc.startOffset {
				pc.logger.Info("Ignoring consume notification smaller than startOffset",
					zap.String("partitionName", pc.p.Name),
					zap.Uint64("newNextOffset", newNextOffset),
					zap.Uint("startOffset", uint(pc.startOffset)))
				continue
			}
			if newNextOffset == pc.nextOffset {
				// pc.logger.Info("No new safe consume offset",
				// 	zap.String("partitionName", pc.p.Name),
				// 	zap.Uint64("newNextOffset", newNextOffset),
				// 	zap.Uint("startOffset", uint(pc.startOffset)))
				continue
			}
			pc.logger.Info("Sending safe consume offset",
				zap.String("partitionName", pc.p.Name),
				zap.Uint64("newNextOffset", newNextOffset))
			pc.sendResponse(&pb.Response{
				Response: &pb.Response_ConsumeResponse{
					ConsumeResponse: &pb.ConsumeResponse{
						EndOfSafeOffsetsExclusively: newNextOffset,
						PartitionName:               pc.p.Name,
					}}})
			pc.nextOffset = newNextOffset
		}
	}
}

func (pc *PartitionConsumer) waitBeforeCheckForNew() {
	time.Sleep(checkForNewLatency)
	select {
	case <-pc.quit:
	case pc.checkForNew <- struct{}{}:
	}
}

func (pc *PartitionConsumer) Close() error {
	close(pc.quit)
	return nil
}
