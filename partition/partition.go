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
	outstandingAcks        chan *outstandingAck
	newCommittedLSN        chan uint64
	logClient              *logclient.ClientWrapper
	logger                 *zap.Logger
	nextLSNCommitted       atomic.Uint64 // initialized to default value 0
	quit                   chan struct{}
}

type LogInteractionRequest struct {
	AppendBatchRequest   *AppendBatchRequest
	pollCommittedRequest *struct{}
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
		outstandingAcks:        make(chan *outstandingAck, 4*logclient.MaxOutstanding),
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

var MaxCheckLSNDelay = 50 * time.Millisecond

// TODO: try passing a batch of messages via cgo interface
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	var lsnAfterLSNForNextAck uint64
	checkScheduled := false
	var passLSN uint8
	for {
		lir, ok := <-p.LogInteractionRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
		if abr := lir.AppendBatchRequest; abr != nil {
			if !checkScheduled {
				go func() {
					time.Sleep(MaxCheckLSNDelay)
					p.LogInteractionRequests <- LogInteractionRequest{
						pollCommittedRequest: &struct{}{},
					}
				}()
				checkScheduled = true
			}
			numMessages := uint32(len(abr.EndOffsetsExclusively))
			p.outstandingAcks <- &outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       abr.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterLSNForNextAck,
					PartitionName: p.Name,
				},
				produceResponse: abr.ProduceResponse,
			}
			lsnAfterLSNForNextAck += uint64(numMessages)
			lsnAfterCommittedLSN, err := p.logClient.AppendAsync(abr.Payload, abr.EndOffsetsExclusively)
			if err != nil {
				p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			passLSN = (passLSN + 1) % 4
			if passLSN == 0 {
				p.newCommittedLSN <- lsnAfterCommittedLSN
			}
		} else if lir.pollCommittedRequest != nil {
			checkScheduled = false
			committedLSN, err := p.logClient.PollCompletion()
			if err != nil {
				p.logger.Error("Error polling committed LSN", zap.Error(err), zap.String("partitionName", p.Name))
			}
			if committedLSN < lsnAfterLSNForNextAck-1 {
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

func (p *Partition) handleAcks() {
	var longestOutstandingAck *outstandingAck
	for {
		committedLSN, ok := <-p.newCommittedLSN
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
		}
		if longestOutstandingAck != nil {
			if committedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
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
			case longestOutstandingAck = <-p.outstandingAcks:
				if committedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
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
