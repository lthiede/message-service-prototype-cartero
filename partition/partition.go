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
	Name               string
	Alive              bool
	AliveLock          sync.RWMutex
	LogInteractionTask chan LogInteractionTask
	logClient          *logclient.ClientWrapper
	logger             *zap.Logger
	newCommittedLSN    chan uint64
	outstandingAcks    chan *outstandingAck
	nextLSNCommitted   atomic.Uint64 // initialized to default value 0
	quit               chan struct{}
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

type outstandingAck struct {
	BatchId         uint64
	MessageId       uint32
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
		LogInteractionTask: make(chan LogInteractionTask),
		quit:               make(chan struct{}),
		newCommittedLSN:    make(chan uint64),
		outstandingAcks:    make(chan *outstandingAck, 128),
		logClient:          logClient,
		logger:             logger,
	}
	go p.logInteractions()
	go p.handleAcks()
	return p, nil
}

var MaxCheckLSNDelay = 50 * time.Millisecond

func (p *Partition) schedulePollCommitted() {
	time.Sleep(MaxCheckLSNDelay)
	p.LogInteractionTask <- LogInteractionTask{
		pollCommittedRequest: &pollCommittedRequest{},
	}
}

// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	checkScheduled := false
	var nextLSNAppended uint64
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
			var lastEndOffsetExclusively uint32
			for i, endOffsetExclusively := range ar.EndOffsetsExclusively {
				for {
					lsn, appendErr := p.logClient.AppendAsync(ar.Payload[lastEndOffsetExclusively:endOffsetExclusively])
					if appendErr == nil {
						nextLSNAppended = lsn + 1
						p.outstandingAcks <- &outstandingAck{
							BatchId:         ar.BatchId,
							MessageId:       uint32(i),
							ProduceResponse: ar.ProduceResponse,
						}
						lastEndOffsetExclusively = endOffsetExclusively
						break
					}
					committedLSN, pollCommittedErr := p.logClient.PollCompletion()
					if pollCommittedErr != nil {
						p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
						continue
					}
					p.newCommittedLSN <- committedLSN
				}
			}
		} else if lit.pollCommittedRequest != nil {
			checkScheduled = false
			committedLSN, pollCommittedErr := p.logClient.PollCompletion()
			if pollCommittedErr != nil {
				p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
			}
			p.newCommittedLSN <- committedLSN
			if committedLSN < uint64(nextLSNAppended-1) {
				go p.schedulePollCommitted()
				checkScheduled = true
			}
		}
	}
}

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
		var currentAck *pb.ProduceAck = nil
		var currentResponseChan chan *pb.Response
		for ; oldNextLSNCommitted+i <= committedLSN; i++ {
			ar := <-p.outstandingAcks
			if currentAck == nil {
				currentAck = &pb.ProduceAck{
					BatchId:        ar.BatchId,
					StartMessageId: ar.MessageId,
					StartLsn:       oldNextLSNCommitted + i,
					NumMessages:    1,
					PartitionName:  p.Name,
				}
				currentResponseChan = ar.ProduceResponse
			} else if currentResponseChan == ar.ProduceResponse && currentAck.BatchId == ar.BatchId {
				currentAck.NumMessages++
			} else {
				currentResponseChan <- &pb.Response{
					Response: &pb.Response_ProduceAck{
						ProduceAck: currentAck,
					},
				}
				currentAck = &pb.ProduceAck{
					BatchId:        ar.BatchId,
					StartMessageId: ar.MessageId,
					StartLsn:       oldNextLSNCommitted + i,
					NumMessages:    1,
					PartitionName:  p.Name,
				}
				currentResponseChan = ar.ProduceResponse
			}

		}
		if currentAck != nil {
			currentResponseChan <- &pb.Response{
				Response: &pb.Response_ProduceAck{
					ProduceAck: currentAck,
				},
			}
		}
		p.logger.Info("Acknowledged committed batches", zap.String("partitionName", p.Name), zap.Uint64("numberAck", committedLSN-oldNextLSNCommitted+1))
		p.nextLSNCommitted.Store(committedLSN + 1)
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
