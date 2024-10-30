package consume

import (
	"time"

	"github.com/lthiede/cartero/partition"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

const checkForNewLatency = 100 * time.Millisecond

type update struct {
	startLSN       uint64
	minNumMessages int
}

type PartitionConsumer struct {
	p              *partition.Partition
	sendResponse   func(*pb.Response)
	startLSN       uint64
	nextLSN        uint64
	minNumMessages int
	logger         *zap.Logger
	update         chan update
	checkForNew    chan struct{}
	quit           chan struct{}
}

func NewPartitionConsumer(p *partition.Partition, sendResponse func(*pb.Response), startLSN uint64, minNumMessages int, logger *zap.Logger) (*PartitionConsumer, error) {
	pc := &PartitionConsumer{
		p:              p,
		sendResponse:   sendResponse,
		startLSN:       startLSN,
		minNumMessages: minNumMessages,
		logger:         logger,
		update:         make(chan update),
		checkForNew:    make(chan struct{}),
		quit:           make(chan struct{}),
	}
	logger.Info("Adding partition consumer", zap.String("partitionName", p.Name), zap.Uint64("startLSN", startLSN))
	go pc.handleConsume()
	return pc, nil
}

func (pc *PartitionConsumer) UpdateConsumption(startLSN uint64, minNumMessages int) {
	pc.logger.Info("Updating partition consumer", zap.String("partitionName", pc.p.Name), zap.Uint64("startLSN", startLSN), zap.Int("minNumMessages", minNumMessages))
	pc.update <- update{
		startLSN:       startLSN,
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
			pc.startLSN = update.startLSN
			pc.minNumMessages = update.minNumMessages
		case <-pc.checkForNew:
			// hallo
			newNextLSN := pc.p.NextLSN()
			if newNextLSN < pc.startLSN {
				pc.logger.Info("Ignoring consume notification smaller than startOffset",
					zap.String("partitionName", pc.p.Name),
					zap.Uint64("newNextLSN", newNextLSN),
					zap.Uint("startLSN", uint(pc.startLSN)))
				continue
			}
			if newNextLSN == pc.nextLSN {
				// pc.logger.Info("No new safe consume offset",
				// 	zap.String("partitionName", pc.p.Name),
				// 	zap.Uint64("newNextOffset", newNextOffset),
				// 	zap.Uint("startOffset", uint(pc.startOffset)))
				continue
			}
			pc.logger.Info("Sending safe consume offset",
				zap.String("partitionName", pc.p.Name),
				zap.Uint64("newNextLSN", newNextLSN))
			pc.sendResponse(&pb.Response{
				Response: &pb.Response_ConsumeResponse{
					ConsumeResponse: &pb.ConsumeResponse{
						EndOfSafeLsnsExclusively: newNextLSN,
						PartitionName:            pc.p.Name,
					}}})
			pc.nextLSN = newNextLSN
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
