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
	acks               chan outstandingAck
	logClient          *logclient.ClientWrapper
	logger             *zap.Logger
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

// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	checkScheduled := false
	outstandingAcks := [logclient.MaxOutstanding]outstandingAck{}
	outstandingAckWriteIndex := 0
	outstandingAckReadIndex := 0
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
			outstandingAcks[outstandingAckWriteIndex] = outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       ar.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterLSNForNextAck,
					PartitionName: p.Name,
				},
				produceResponse: ar.ProduceResponse,
			}
			outstandingAckWriteIndex = (outstandingAckWriteIndex + 1) % logclient.MaxOutstanding
			lsnAfterLSNForNextAck += uint64(numMessages)
			var lastEndOffsetExclusively uint32
			for _, endOffsetExclusively := range ar.EndOffsetsExclusively {
				for {
					_, appendErr := p.logClient.AppendAsync(ar.Payload[lastEndOffsetExclusively:endOffsetExclusively])
					if appendErr == nil {
						lastEndOffsetExclusively = endOffsetExclusively
						break
					}
					p.pollCommitted(outstandingAcks[outstandingAckReadIndex])
				}
			}
		} else if lit.pollCommittedRequest != nil {
			checkScheduled = false
			committedLSN, err := p.pollCommitted(outstandingAcks[outstandingAckReadIndex])
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

func (p *Partition) pollCommitted(outstandingAck outstandingAck) (uint64, error) {
	committedLSN, pollCommittedErr := p.logClient.PollCompletion()
	if pollCommittedErr != nil {
		p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
		return 0, pollCommittedErr
	}
	if committedLSN+1 >= outstandingAck.ack.StartLsn+uint64(outstandingAck.ack.NumMessages) {
		p.acks <- outstandingAck
	}
	p.nextLSNCommitted.Store(committedLSN + 1)
	return committedLSN, nil
}

func (p *Partition) NextLSN() uint64 {
	return p.nextLSNCommitted.Load()
}

func (p *Partition) Close() error {
	p.logger.Debug("Closing partition", zap.String("partitionName", p.Name))
	close(p.quit)
	return nil
}
