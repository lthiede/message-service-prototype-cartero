package partition

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"google.golang.org/protobuf/proto"

	//logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"

	"go.uber.org/zap"
)

const MaxBatchBytesSize = 3800

type Partition struct {
	Name               string
	Alive              bool
	AliveLock          sync.RWMutex
	LogInteractionTask chan LogInteractionTask
	//logClient          *logclient.ClientWrapper
	logger           *zap.Logger
	newCommittedLSN  chan uint64
	outstandingAcks  chan *ProduceRequest
	nextLSNCommitted atomic.Uint64 // initialized to default value 0
	quit             chan struct{}
}

type LogInteractionTask struct {
	ProduceRequest       *ProduceRequest
	pollCommittedRequest *pollCommittedRequest
}

type ProduceRequest struct {
	BatchId         uint64
	Messages        *pb.Messages
	ProduceResponse chan *pb.Response
}

type pollCommittedRequest struct {
}

type ConsumeRequest struct {
	Quit   chan struct{}
	Notify chan uint64
}

func New(name string, logAddresses []string, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partitionName", name))
	// logClient, err := logclient.New(logAddresses, logclient.MaxOutstanding, logclient.UringEntries, logclient.UringFlagNoSingleIssuer)
	// if err != nil {
	// 	return nil, fmt.Errorf("error creating log client: %v", err)
	// }
	// err = logClient.Connect()
	// if err != nil {
	// 	return nil, fmt.Errorf("error connecting to log nodes: %v", err)
	// }
	p := &Partition{
		Name:               name,
		Alive:              true,
		LogInteractionTask: make(chan LogInteractionTask),
		quit:               make(chan struct{}),
		newCommittedLSN:    make(chan uint64),
		outstandingAcks:    make(chan *ProduceRequest, 128),
		// logClient:          logClient,
		logger: logger,
	}
	go p.logInteractions()
	go p.handleAcks()
	return p, nil
}

var MaxCheckLSNDelay = 50 * time.Millisecond

func messageBytes(produceRequest *ProduceRequest) ([]byte, error) {
	if proto.Size(produceRequest.Messages) > MaxBatchBytesSize {
		return nil, fmt.Errorf("got batch with too many bytes. got %d bytes, max %d bytes", proto.Size(produceRequest.Messages), MaxBatchBytesSize)
	}
	message, err := proto.Marshal(produceRequest.Messages)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message batch: %v", err)
	}
	return message, nil
}

func (p *Partition) schedulePollCommitted() {
	time.Sleep(MaxCheckLSNDelay)
	p.LogInteractionTask <- LogInteractionTask{
		pollCommittedRequest: &pollCommittedRequest{},
	}
}

// Don't add a channel select to this function
// Not remarshaling the message batch could slightly raise throughput if it can be done
// Receiving on the channel is not a bottleneck
// Raising the message rate at connection could massively increase the throughput
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	var nextLSNAppended uint64 = 0
	checkScheduled := false
	for {
		lit, ok := <-p.LogInteractionTask
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
		if lit.ProduceRequest != nil {
			pr := lit.ProduceRequest
			// message, err := messageBytes(pr)
			// if err != nil {
			// 	p.logger.Error("Failed to get message bytes",
			// 		zap.Error(err),
			// 		zap.Uint64("batchId", pr.BatchId),
			// 		zap.String("partitionName", p.Name))
			// }
			// for {
			// lsn, appendErr := p.logClient.AppendAsync(payload)
			// if appendErr == nil {
			// 	nextLSNAppended = lsn + 1
			// 	p.logger.Info("Sent batch to log", zap.String("partitionName", p.Name), zap.Uint64("lsn", lsn), zap.Uint64("batchId", pr.BatchId), zap.Int("numberMessages", len(pr.Messages.Messages)))
			if (nextLSNAppended+1)%128 == 0 {
				p.newCommittedLSN <- nextLSNAppended - 1
			}
			p.outstandingAcks <- pr
			nextLSNAppended++
			if !checkScheduled {
				go p.schedulePollCommitted()
				checkScheduled = true
			}
			// 	break
			// }
			// p.logger.Warn("Failed to send message batch. Waiting for commits",
			// 	zap.Error(appendErr),
			// 	zap.Uint64("batchId", pr.BatchId),
			// 	zap.String("partitionName", p.Name))
			// p.logger.Info("Checking committed batches, because reached max outstanding", zap.String("partitionName", p.Name))
			// _, pollCommittedErr := p.logClient.PollCompletion()
			// if pollCommittedErr != nil {
			// 	p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
			// 	continue
			// }
			// p.newCommittedLSN <- committedLSN
			// p.logger.Info("Retrying failed message batch", zap.String("partitionName", p.Name))

			// }
		} else if lit.pollCommittedRequest != nil {
			checkScheduled = false
			// p.logger.Info("Checking committed batches, because reached max check delay", zap.String("partitionName", p.Name))
			// committedLSN, pollCommittedErr := p.logClient.PollCompletion()
			// if pollCommittedErr != nil {
			// 	p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
			// }
			// p.newCommittedLSN <- committedLSN
			p.newCommittedLSN <- nextLSNAppended - 1
			// if committedLSN < uint64(nextLSNAppended-1) {
			// 	go p.schedulePollCommitted()
			// 	checkScheduled = true
			// }
		}
	}
}

// Don't add a channel select to this function
// Not remarshaling the message batch could slightly raise throughput if it can be done
// Receiving on the channel is not a bottleneck
// Raising the message rate at connection could massively increase the throughput
// func (p *Partition) logInteractions() {
// 	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
// 	checkScheduled := false
// 	var nextLSNAppended uint64
// 	for {
// 		lit, ok := <-p.LogInteractionTask
// 		if !ok {
// 			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
// 			return
// 		}
// 		if lit.ProduceRequest != nil {
// 			pr := lit.ProduceRequest
// 			message, err := messageBytes(pr)
// 			if err != nil {
// 				p.logger.Error("Failed to get message bytes",
// 					zap.Error(err),
// 					zap.Uint64("batchId", pr.BatchId),
// 					zap.String("partitionName", p.Name))
// 			}
// 			for {
// 				lsn, appendErr := p.logClient.AppendAsync(message)
// 				if appendErr == nil {
// 					nextLSNAppended = lsn + 1
// 					p.outstandingAcks <- lit.ProduceRequest
// 					break
// 				}
// 				committedLSN, pollCommittedErr := p.logClient.PollCompletion()
// 				if pollCommittedErr != nil {
// 					p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
// 					continue
// 				}
// 				p.newCommittedLSN <- committedLSN
// 			}
// 			if !checkScheduled {
// 				go func() {
// 					time.Sleep(MaxCheckLSNDelay)
// 					p.LogInteractionTask <- LogInteractionTask{
// 						pollCommittedRequest: &pollCommittedRequest{},
// 					}
// 				}()
// 			}
// 		} else if lit.pollCommittedRequest != nil {
// 			checkScheduled = false
// 			committedLSN, pollCommittedErr := p.logClient.PollCompletion()
// 			if pollCommittedErr != nil {
// 				p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
// 			}
// 			p.newCommittedLSN <- committedLSN
// 			if committedLSN < uint64(nextLSNAppended-1) {
// 				go p.schedulePollCommitted()
// 				checkScheduled = true
// 			}
// 		}
// 	}
// }

// func (p *Partition) handleAcks() {
// 	p.logger.Info("Start handling acks", zap.String("partitionName", p.Name))
// 	for {
// 		committedLSN, ok := <-p.newCommittedLSN
// 		if !ok {
// 			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
// 			return
// 		}
// 		p.logger.Info("Acknowledge committed batches", zap.Uint64("lsn", committedLSN), zap.String("partitionName", p.Name))
// 		oldNextLSNCommitted := p.nextLSNCommitted.Load()
// 		var i uint64
// 		for ; oldNextLSNCommitted+i <= committedLSN; i++ {
// 			pr := <-p.outstandingAcks
// 			pr.ProduceResponse <- &pb.Response{
// 				Response: &pb.Response_ProduceAck{
// 					ProduceAck: &pb.ProduceAck{
// 						BatchId:       pr.BatchId,
// 						Lsn:           oldNextLSNCommitted + i,
// 						NumMessages:   uint32(len(pr.Messages.Messages)),
// 						PartitionName: p.Name,
// 					},
// 				},
// 			}
// 		}
// 		p.logger.Info("Acknowledged committed batches", zap.String("partitionName", p.Name), zap.Uint64("numberAck", committedLSN-oldNextLSNCommitted+1))
// 		p.nextLSNCommitted.Store(committedLSN + 1)
// 	}
// }

func (p *Partition) handleAcks() {
	p.logger.Info("Start handling acks", zap.String("partitionName", p.Name))
	for {
		committedLSN, ok := <-p.newCommittedLSN
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
			return
		}
		p.logger.Info("Acknowledge committed batches", zap.Uint64("lsn", committedLSN), zap.String("partitionName", p.Name))
		oldNextLSNCommitted := p.nextLSNCommitted.Load()
		var i uint64
		for ; oldNextLSNCommitted+i <= committedLSN; i++ {
			pr := <-p.outstandingAcks
			pr.ProduceResponse <- &pb.Response{
				Response: &pb.Response_ProduceAck{
					ProduceAck: &pb.ProduceAck{
						BatchId:       pr.BatchId,
						Lsn:           oldNextLSNCommitted + i,
						NumMessages:   uint32(len(pr.Messages.Messages)),
						PartitionName: p.Name,
					},
				},
			}
		}
		p.logger.Info("Acknowledged committed batches", zap.String("partitionName", p.Name), zap.Uint64("numberAck", committedLSN-oldNextLSNCommitted+1))
		p.nextLSNCommitted.Store(committedLSN + 1)
	}
}

func (p *Partition) acknowledgeCommitted(committedLSN uint64) {
	oldNextLSNCommitted := p.nextLSNCommitted.Load()
	var i uint64
	for ; i <= committedLSN-oldNextLSNCommitted; i++ {
		pr := <-p.outstandingAcks
		pr.ProduceResponse <- &pb.Response{
			Response: &pb.Response_ProduceAck{
				ProduceAck: &pb.ProduceAck{
					BatchId:       pr.BatchId,
					Lsn:           oldNextLSNCommitted + i,
					NumMessages:   uint32(len(pr.Messages.Messages)),
					PartitionName: p.Name,
				},
			},
		}
	}
	p.logger.Info("Acknowledged committed batches", zap.String("partitionName", p.Name), zap.Uint64("numberAck", committedLSN-oldNextLSNCommitted+1))
	p.nextLSNCommitted.Store(committedLSN + 1)
}

func (p *Partition) NextLSN() uint64 {
	return p.nextLSNCommitted.Load()
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	return nil
}
