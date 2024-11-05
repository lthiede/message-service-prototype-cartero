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
	Name                     string
	Alive                    bool
	AliveLock                sync.RWMutex
	LogInteractionTask       chan LogInteractionTask
	acks                     chan outstandingAck
	outstandingAcks          []outstandingAck
	outstandingAckWriteIndex int
	outstandingAckReadIndex  int
	logClient                *logclient.ClientWrapper
	logger                   *zap.Logger
	nextLSNCommitted         atomic.Uint64 // initialized to default value 0
	quit                     chan struct{}
}

type LogInteractionTask struct {
	AppendMessageRequest *AppendMessageRequest
	pollCommittedRequest *pollCommittedRequest
}

type AppendMessageRequest struct {
	BatchId               uint64
	EndOffsetsExclusively []uint32
	Payload               []byte
	ProduceResponse       chan *pb.Response
}

type pollCommittedRequest struct {
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
		Name:               name,
		Alive:              true,
		acks:               make(chan outstandingAck, logclient.MaxOutstanding),
		outstandingAcks:    make([]outstandingAck, logclient.MaxOutstanding+1),
		LogInteractionTask: make(chan LogInteractionTask),
		quit:               make(chan struct{}),
		logClient:          logClient,
		logger:             logger,
	}
	go p.logInteractions()
	go p.sendAcks()
	return p, nil
}

var MaxCheckLSNDelay = 50 * time.Millisecond

func (p *Partition) schedulePollCommitted() {
	time.Sleep(MaxCheckLSNDelay)
	p.LogInteractionTask <- LogInteractionTask{
		pollCommittedRequest: &pollCommittedRequest{},
	}
}

// TODO: try passing a batch of messages via cgo interface
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	checkScheduled := false
	var lsnSimulation uint64
	var lsnAfterLSNForNextAck uint64
	for {
		lit, ok := <-p.LogInteractionTask
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
		}
		if lit.AppendMessageRequest != nil {
			ar := lit.AppendMessageRequest
			if !checkScheduled {
				go p.schedulePollCommitted()
				checkScheduled = true
			}
			numMessages := uint32(len(ar.EndOffsetsExclusively))
			p.outstandingAcks[p.outstandingAckWriteIndex] = outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       ar.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterLSNForNextAck,
					PartitionName: p.Name,
				},
				produceResponse: ar.ProduceResponse,
			}
			p.outstandingAckWriteIndex = (p.outstandingAckWriteIndex + 1) % (logclient.MaxOutstanding + 1)
			lsnAfterLSNForNextAck += uint64(numMessages)
			// var lastEndOffsetExclusively uint32
			// for _, endOffsetExclusively := range ar.EndOffsetsExclusively {
			for range ar.EndOffsetsExclusively {
				polled := false
				for {
					// _, appendErr := p.logClient.AppendAsync(ar.Payload[lastEndOffsetExclusively:endOffsetExclusively])
					// if appendErr == nil {
					if (lsnSimulation+1)%logclient.MaxOutstanding != 0 || polled {
						lsnSimulation++
						// lastEndOffsetExclusively = endOffsetExclusively
						break
					}
					polled = true
					p.pollCommitted(lsnSimulation)
				}
			}
		} else if lit.pollCommittedRequest != nil {
			checkScheduled = false
			committedLSN, err := p.pollCommitted(lsnSimulation)
			if err != nil {
				continue
			}
			if committedLSN < uint64(lsnAfterLSNForNextAck-1) {
				go p.schedulePollCommitted()
				checkScheduled = true
			}
		}
	}
}

func (p *Partition) pollCommitted(lsnSimulation uint64) (uint64, error) {
	// committedLSN, pollCommittedErr := p.logClient.PollCompletion()
	// if pollCommittedErr != nil {
	// 	p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
	// 	return 0, pollCommittedErr
	// }
	outstandingAck := p.outstandingAcks[p.outstandingAckReadIndex]
	for p.outstandingAckReadIndex != p.outstandingAckWriteIndex && lsnSimulation+1 >= outstandingAck.ack.StartLsn+uint64(outstandingAck.ack.NumMessages) {
		p.acks <- outstandingAck
		p.outstandingAckReadIndex = (p.outstandingAckReadIndex + 1) % (logclient.MaxOutstanding + 1)
		outstandingAck = p.outstandingAcks[p.outstandingAckReadIndex]
	}
	p.nextLSNCommitted.Store(lsnSimulation + 1)
	return lsnSimulation, nil
}

func (p *Partition) sendAcks() {
	for {
		ack, ok := <-p.acks
		if !ok {
			p.logger.Info("Stop sending acks", zap.String("partitionName", p.Name))
			return
		}
		ack.produceResponse <- &pb.Response{
			Response: &pb.Response_ProduceAck{
				ProduceAck: ack.ack,
			},
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
