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
	Name                  string
	Alive                 bool
	AliveLock             sync.RWMutex
	AppendBatchRequest    chan AppendBatchRequest
	outstandingAcks       chan *outstandingAck
	pollCommittedRequests chan pollCommittedRequest
	logClient             *logclient.ClientWrapper
	logClientLock         sync.Mutex
	logger                *zap.Logger
	nextLSNCommitted      atomic.Uint64 // initialized to default value 0
	quit                  chan struct{}
}

type AppendBatchRequest struct {
	BatchId               uint64
	EndOffsetsExclusively []uint32
	Payload               []byte
	ProduceResponse       chan *pb.Response
}

type pollCommittedRequest struct {
	scheduledCheck bool
	polled         chan struct{}
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
		Name:                  name,
		Alive:                 true,
		outstandingAcks:       make(chan *outstandingAck, logclient.MaxOutstanding+1),
		pollCommittedRequests: make(chan pollCommittedRequest),
		AppendBatchRequest:    make(chan AppendBatchRequest),
		quit:                  make(chan struct{}),
		logClient:             logClient,
		logger:                logger,
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
	waitForCommitsPolled := make(chan struct{})
	for {
		abr, ok := <-p.AppendBatchRequest
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			return
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
		var lastEndOffsetExclusively uint32
		for _, endOffsetExclusively := range abr.EndOffsetsExclusively {
			for {
				p.logClientLock.Lock()
				_, appendErr := p.logClient.AppendAsync(abr.Payload[lastEndOffsetExclusively:endOffsetExclusively])
				p.logClientLock.Unlock()
				if appendErr == nil {
					lastEndOffsetExclusively = endOffsetExclusively
					break
				}
				p.pollCommittedRequests <- pollCommittedRequest{
					scheduledCheck: false,
					polled:         waitForCommitsPolled,
				}
				<-waitForCommitsPolled
			}
		}
	}
}

func (p *Partition) scheduleCheck() {
	go func() {
		time.Sleep(MaxCheckLSNDelay)
		p.pollCommittedRequests <- pollCommittedRequest{
			scheduledCheck: true,
			polled:         nil,
		}
	}()
}

func (p *Partition) handleAcks() {
	var current *outstandingAck
	checkScheduled := false
	for {
		if current != nil && !checkScheduled {
			p.scheduleCheck()
			checkScheduled = true
		}
		pollCommittedRequest, ok := <-p.pollCommittedRequests
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
			return
		}
		if pollCommittedRequest.scheduledCheck {
			checkScheduled = false
		}
		p.logClientLock.Lock()
		committedLSN, pollCommittedErr := p.logClient.PollCompletion()
		p.logClientLock.Unlock()
		if pollCommittedErr != nil {
			p.logger.Error("Error polling for committed lsn", zap.Error(pollCommittedErr), zap.String("partitionName", p.Name))
			continue
		}
		if pollCommittedRequest.polled != nil {
			pollCommittedRequest.polled <- struct{}{}
		}
		if current != nil {
			if committedLSN+1 >= current.ack.StartLsn+uint64(current.ack.NumMessages) {
				current.produceResponse <- &pb.Response{
					Response: &pb.Response_ProduceAck{
						ProduceAck: current.ack,
					},
				}
				current = nil
			} else {
				continue
			}
		}
	ackCommittedBatches:
		for {
			select {
			case current = <-p.outstandingAcks:
				if committedLSN+1 >= current.ack.StartLsn+uint64(current.ack.NumMessages) {
					current.produceResponse <- &pb.Response{
						Response: &pb.Response_ProduceAck{
							ProduceAck: current.ack,
						},
					}
					current = nil
				} else {
					break ackCommittedBatches
				}
			default:
				break
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
