package consume

import (
	"errors"
	"math"
	"sync"
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
	sendResponse   func(*pb.Response) error
	startLSN       uint64
	nextLSN        uint64
	minNumMessages int
	logger         *zap.Logger
	update         chan update
	checkForNew    chan struct{}
	Alive          bool
	AliveLock      sync.RWMutex
	quit           chan struct{}
}

func NewPartitionConsumer(p *partition.Partition, sendResponse func(*pb.Response) error, startLSN uint64, minNumMessages int, logger *zap.Logger) (*PartitionConsumer, error) {
	pc := &PartitionConsumer{
		p:              p,
		sendResponse:   sendResponse,
		startLSN:       startLSN,
		minNumMessages: minNumMessages,
		logger:         logger,
		update:         make(chan update),
		checkForNew:    make(chan struct{}),
		Alive:          true,
		quit:           make(chan struct{}),
	}
	logger.Info("Adding partition consumer", zap.String("partitionName", p.Name), zap.Uint64("startLSN", startLSN))
	go pc.handleConsume()
	return pc, nil
}

func (pc *PartitionConsumer) UpdateConsumption(startLSN uint64, minNumMessages int) error {
	pc.logger.Info("Updating partition consumer", zap.String("partitionName", pc.p.Name), zap.Uint64("startLSN", startLSN), zap.Int("minNumMessages", minNumMessages))
	pc.AliveLock.Lock()
	defer pc.AliveLock.Unlock()
	if !pc.Alive {
		return errors.New("Consumption is dead")
	}
	pc.update <- update{
		startLSN:       startLSN,
		minNumMessages: minNumMessages,
	}
	return nil
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
			newNextLSN := pc.p.NextLSN()
			if newNextLSN == math.MaxUint64 {
				pc.Close()
				continue
			}
			if newNextLSN <= pc.startLSN {
				// pc.logger.Info("Ignoring consume offset smaller than startOffset",
				// 	zap.String("partitionName", pc.p.Name),
				// 	zap.Uint64("newNextLSN", newNextLSN),
				// 	zap.Uint("startLSN", uint(pc.startLSN)))
				continue
			}
			if newNextLSN == pc.nextLSN {
				// pc.logger.Info("Ignoring consume notification equal to last offset",
				// 	zap.String("partitionName", pc.p.Name),
				// 	zap.Uint64("newNextLSN", newNextLSN))
				continue
			}
			// pc.logger.Info("Sending safe consume offset",
			// 	zap.String("partitionName", pc.p.Name),
			// 	zap.Uint64("newNextLSN", newNextLSN))
			err := pc.sendResponse(&pb.Response{
				Response: &pb.Response_ConsumeResponse{
					ConsumeResponse: &pb.ConsumeResponse{
						EndOfSafeLsnsExclusively: newNextLSN,
						PartitionName:            pc.p.Name,
					}}})
			if err != nil {
				pc.Close()
				continue
			}
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
	pc.logger.Info(("Closing consumption"))
	pc.AliveLock.Lock()
	defer pc.AliveLock.Unlock()
	if pc.Alive {
		pc.Alive = false
	} else {
		return nil
	}
	close(pc.quit)
	return nil
}
