package partition

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"

	logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"

	"go.uber.org/zap"
)

const MaxMessageSize = 3800

type Partition struct {
	Name                   string
	Alive                  bool
	AliveLock              sync.RWMutex
	LogInteractionRequests chan LogInteractionRequest
	numOutstanding         uint
	numOutstandingLock     sync.Mutex
	outstandingAcks        chan *outstandingAck
	newCommittedLSN        chan uint64
	logClient              *logclient.ClientWrapper
	logger                 *zap.Logger
	nextLSNCommitted       atomic.Uint64 // initialized to default value 0
	quit                   chan struct{}
}

type LogInteractionRequest struct {
	AppendBatchRequest   *AppendBatchRequest
	flushBatchRequest    *flushBatchRequest
	pollCommittedRequest *struct{}
}

type flushBatchRequest struct {
	epoch int
}

type AppendBatchRequest struct {
	BatchId               uint64
	EndOffsetsExclusively []uint32
	Payload               []byte
	ProduceResponse       chan *pb.Response
}

type outstandingAck struct {
	ack             *pb.ProduceAck
	produceResponse chan *pb.Response
}

type ConsumeRequest struct {
	Quit   chan struct{}
	Notify chan uint64
}

func New(name string, logAddresses []string, logger *zap.Logger) (*Partition, error) {
	logger.Info("Creating new partition", zap.String("partitionName", name))
	logClient, err := logclient.New(logAddresses, logclient.MaxOutstanding, logclient.UringEntries, logclient.UringFlagNoSingleIssuer)
	if err != nil {
		return nil, fmt.Errorf("error creating log client: %v", err)
	}
	err = logClient.Connect()
	if err != nil {
		return nil, fmt.Errorf("error connecting to log nodes: %v", err)
	}
	p := &Partition{
		Name:                   name,
		Alive:                  true,
		outstandingAcks:        make(chan *outstandingAck, logclient.MaxOutstanding+1),
		newCommittedLSN:        make(chan uint64),
		LogInteractionRequests: make(chan LogInteractionRequest),
		quit:                   make(chan struct{}),
		logClient:              logClient,
		logger:                 logger,
	}
	go p.logInteractions()
	go p.handleAcks()
	return p, nil
}

var MaxCheckLSNDelay = 250 * time.Millisecond
var MaxFlushBatchDelay = 100 * time.Millisecond

// TODO: continue with changes to segmentstore
// TODO: build segmentstore in release mode
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	batches := make([][]byte, 0, 16)
	endOffsets := make([][]uint32, 0, 16)
	var lsnAfterHighestReceivedLSN uint64
	var lsnAfterHighestAppendedLSN uint64
	flushEpoch := 0
	flushScheduled := false
	checkScheduled := false
	for {
		lir, ok := <-p.LogInteractionRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
		if abr := lir.AppendBatchRequest; abr != nil {
			p.numOutstandingLock.Lock()
			currentNumOutstanding := p.numOutstanding
			p.numOutstandingLock.Unlock()
			if currentNumOutstanding == logclient.MaxOutstanding+1 {
				if !checkScheduled {
					go func() {
						time.Sleep(MaxCheckLSNDelay)
						p.LogInteractionRequests <- LogInteractionRequest{
							pollCommittedRequest: &struct{}{},
						}
					}()
					checkScheduled = true
				}
				lsnAfterCommittedLSN, err := p.logClient.AppendAsync(batches, endOffsets)
				if err != nil {
					p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
					return
				}
				batches = make([][]byte, 0, 16)
				endOffsets = make([][]uint32, 0, 16)
				lsnAfterCommittedLSN = lsnAfterHighestReceivedLSN
				if lsnAfterCommittedLSN == 0 {
					panic("appended batches, but no lsn committed")
				}
				p.newCommittedLSN <- lsnAfterCommittedLSN
				flushEpoch++
			}
			numMessages := uint32(len(abr.EndOffsetsExclusively))
			p.outstandingAcks <- &outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       abr.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterHighestReceivedLSN,
					PartitionName: p.Name,
				},
				produceResponse: abr.ProduceResponse,
			}
			p.numOutstandingLock.Lock()
			p.numOutstanding++
			p.numOutstandingLock.Unlock()
			if !flushScheduled {
				go func(epoch int) {
					time.Sleep(MaxFlushBatchDelay)
					p.LogInteractionRequests <- LogInteractionRequest{
						flushBatchRequest: &flushBatchRequest{
							epoch: epoch,
						},
					}
				}(flushEpoch)
				flushScheduled = true
			}
			lsnAfterHighestReceivedLSN += uint64(numMessages)
			batches = append(batches, abr.Payload)
			endOffsets = append(endOffsets, abr.EndOffsetsExclusively)
		} else if fbr := lir.flushBatchRequest; fbr != nil {
			flushScheduled = false
			if fbr.epoch < flushEpoch {
				continue
			}
			if !checkScheduled {
				go func() {
					time.Sleep(MaxCheckLSNDelay)
					p.LogInteractionRequests <- LogInteractionRequest{
						pollCommittedRequest: &struct{}{},
					}
				}()
				checkScheduled = true
			}
			lsnAfterCommittedLSN, err := p.logClient.AppendAsync(batches, endOffsets)
			if err != nil {
				p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			batches = make([][]byte, 0, 16)
			endOffsets = make([][]uint32, 0, 16)
			lsnAfterHighestAppendedLSN = lsnAfterHighestReceivedLSN
			if lsnAfterCommittedLSN == 0 {
				panic("appended batches, but no lsn committed")
			}
			p.newCommittedLSN <- lsnAfterCommittedLSN
		} else if lir.pollCommittedRequest != nil {
			checkScheduled = false
			committedLSN, err := p.logClient.PollCompletion()
			if err != nil {
				p.logger.Error("Error polling committed LSN", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			if committedLSN < lsnAfterHighestAppendedLSN-1 {
				go func() {
					time.Sleep(MaxCheckLSNDelay)
					p.LogInteractionRequests <- LogInteractionRequest{
						pollCommittedRequest: &struct{}{},
					}
				}()
				checkScheduled = true
			}
			p.newCommittedLSN <- committedLSN + 1
		}
	}
}

func (p *Partition) flush() {

}

func (p *Partition) handleAcks() {
	var longestOutstandingAck *outstandingAck
	for {
		lsnAfterCommittedLSN, ok := <-p.newCommittedLSN
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
		}
		if longestOutstandingAck != nil {
			if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
				longestOutstandingAck.produceResponse <- &pb.Response{
					Response: &pb.Response_ProduceAck{
						ProduceAck: longestOutstandingAck.ack,
					},
				}
			} else {
				continue
			}
		}
	moreAcks:
		for {
			select {
			case longestOutstandingAck, ok = <-p.outstandingAcks:
				p.numOutstandingLock.Lock()
				p.numOutstanding--
				p.numOutstandingLock.Unlock()
				if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
					longestOutstandingAck.produceResponse <- &pb.Response{
						Response: &pb.Response_ProduceAck{
							ProduceAck: longestOutstandingAck.ack,
						},
					}
				} else {
					break moreAcks
				}
			default:
				longestOutstandingAck = nil
				break moreAcks
			}
		}
	}
}

func (p *Partition) NextLSN() uint64 {
	return p.nextLSNCommitted.Load()
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	return nil
}
