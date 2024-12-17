package partition

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"

	logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"

	"go.uber.org/zap"
)

const MaxMessageSize = 3800

type Partition struct {
	Name                    string
	Alive                   bool
	AliveLock               sync.RWMutex
	LogInteractionRequests  chan LogInteractionRequest
	outstandingAcks         chan *outstandingAck
	newCommittedLSN         chan uint64
	logClient               *logclient.ClientWrapper
	logger                  *zap.Logger
	nextLSNCommitted        atomic.Uint64 // initialized to default value 0
	pollCommittedGoRoutines atomic.Uint64
}

type LogInteractionRequest struct {
	AppendBatchRequest   *AppendBatchRequest
	pollCommittedRequest *struct{}
}

type AppendBatchRequest struct {
	BatchId               uint64
	EndOffsetsExclusively []uint32
	Payload               []byte
	Alive                 *bool
	AliveLock             *sync.RWMutex
	ProduceResponse       chan *pb.Response
}

type outstandingAck struct {
	ack             *pb.ProduceAck
	produceResponse chan *pb.Response
	alive           *bool
	aliveLock       *sync.RWMutex
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
		logClient:              logClient,
		logger:                 logger,
	}
	go p.logInteractions()
	go p.handleAcks()
	return p, nil
}

// var MaxCheckLSNDelay = 200 * time.Microsecond

// TODO: try passing a batch of messages via cgo interface
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	var lsnAfterLastPolledCommittedLSN uint64
	var lsnAfterMostRecentLSN uint64
	checkScheduled := false
	for {
		lir, ok := <-p.LogInteractionRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			close(p.newCommittedLSN)
			close(p.outstandingAcks)
			return
		}
		if abr := lir.AppendBatchRequest; abr != nil {
			numMessages := uint32(len(abr.EndOffsetsExclusively))
			p.logger.Info("Trying to add outstanding", zap.String("partitionName", p.Name))
			p.outstandingAcks <- &outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       abr.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterMostRecentLSN,
					PartitionName: p.Name,
				},
				produceResponse: abr.ProduceResponse,
				alive:           abr.Alive,
				aliveLock:       abr.AliveLock,
			}
			p.logger.Info("Added outstanding", zap.String("partitionName", p.Name))
			lsnAfterMostRecentLSN += uint64(numMessages)
			lsnAfterCommittedLSN, err := p.logClient.AppendAsync(abr.Payload, abr.EndOffsetsExclusively)
			if err != nil {
				p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			if lsnAfterCommittedLSN != lsnAfterMostRecentLSN && !checkScheduled {
				p.pollCommittedGoRoutines.Add(1)
				go func() {
					// time.Sleep(MaxCheckLSNDelay)
					p.AliveLock.RLock()
					defer p.AliveLock.RUnlock()
					if p.Alive {
						p.LogInteractionRequests <- LogInteractionRequest{
							pollCommittedRequest: &struct{}{},
						}
					}
				}()
				checkScheduled = true
			}
			if lsnAfterCommittedLSN != 0 && lsnAfterCommittedLSN != lsnAfterLastPolledCommittedLSN {
				p.logger.Info("Sending lsn", zap.String("partitionName", p.Name))
				p.newCommittedLSN <- lsnAfterCommittedLSN
				p.logger.Info("Sent lsn", zap.String("partitionName", p.Name))
				lsnAfterLastPolledCommittedLSN = lsnAfterCommittedLSN
			}
		} else if lir.pollCommittedRequest != nil {
			p.pollCommittedGoRoutines.Store(p.pollCommittedGoRoutines.Load() - 1)
			checkScheduled = false
			committedLSN, err := p.logClient.PollCompletion()
			if err != nil {
				p.logger.Error("Error polling committed LSN", zap.Error(err), zap.String("partitionName", p.Name))
			}
			if err != nil || committedLSN+1 < lsnAfterMostRecentLSN {
				p.pollCommittedGoRoutines.Add(1)
				go func() {
					// time.Sleep(MaxCheckLSNDelay)
					p.AliveLock.RLock()
					defer p.AliveLock.RUnlock()
					if p.Alive {
						p.LogInteractionRequests <- LogInteractionRequest{
							pollCommittedRequest: &struct{}{},
						}
					}
				}()
				checkScheduled = true
			}
			if err == nil && committedLSN+1 != lsnAfterLastPolledCommittedLSN {
				p.logger.Info("Sending lsn", zap.String("partitionName", p.Name))
				p.newCommittedLSN <- committedLSN + 1
				p.logger.Info("Sent lsn", zap.String("partitionName", p.Name))
				lsnAfterLastPolledCommittedLSN = committedLSN + 1
			}
		}
	}
}

func pct(latencies []time.Duration, pct float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	ordinal := int(math.Ceil(float64(len(latencies)) * pct))
	ordinalZeroIndexed := ordinal - 1
	return latencies[ordinalZeroIndexed].Seconds()
}

func (p *Partition) handleAcks() {
	var longestOutstandingAck *outstandingAck
	for {
		p.logger.Info("receiving lsn", zap.String("partitionName", p.Name))
		lsnAfterCommittedLSN, ok := <-p.newCommittedLSN
		p.logger.Info("got lsn", zap.String("partitionName", p.Name))
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
			p.nextLSNCommitted.Store(math.MaxUint64)
			return
		}
		p.nextLSNCommitted.Store(lsnAfterCommittedLSN)
		if longestOutstandingAck != nil {
			if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
				p.logger.Info("locking response", zap.String("partitionName", p.Name))
				longestOutstandingAck.aliveLock.RLock()
				p.logger.Info("locked response", zap.String("partitionName", p.Name),
					zap.String("alive", fmt.Sprintf("%t %p", *longestOutstandingAck.alive, longestOutstandingAck.alive)))
				if *longestOutstandingAck.alive {
					p.logger.Info("sending response", zap.String("partitionName", p.Name))
					longestOutstandingAck.produceResponse <- &pb.Response{
						Response: &pb.Response_ProduceAck{
							ProduceAck: longestOutstandingAck.ack,
						},
					}
					p.logger.Info("sent response", zap.String("partitionName", p.Name))
				}
				longestOutstandingAck.aliveLock.RUnlock()
			} else {
				continue
			}
		}
	moreAcks:
		for {
			p.logger.Info("maybe outstanding", zap.String("partitionName", p.Name))
			select {
			case longestOutstandingAck, ok = <-p.outstandingAcks:
				p.logger.Info("got outstanding", zap.String("partitionName", p.Name))
				if !ok {
					p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
					p.nextLSNCommitted.Store(math.MaxUint64)
					return
				}
				if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
					p.logger.Info("locking response", zap.String("partitionName", p.Name))
					longestOutstandingAck.aliveLock.RLock()
					p.logger.Info("locked response", zap.String("partitionName", p.Name),
						zap.String("alive", fmt.Sprintf("%t %p", *longestOutstandingAck.alive, longestOutstandingAck.alive)))
					if *longestOutstandingAck.alive {
						p.logger.Info("sending response", zap.String("partitionName", p.Name))
						longestOutstandingAck.produceResponse <- &pb.Response{
							Response: &pb.Response_ProduceAck{
								ProduceAck: longestOutstandingAck.ack,
							},
						}
						p.logger.Info("sent response", zap.String("partitionName", p.Name))
					}
					longestOutstandingAck.aliveLock.RUnlock()
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
	for {
		p.logger.Info("Trying to acquire partition lock",
			zap.String("partitionName", p.Name),
			zap.Uint64("competingPollGoRoutines", p.pollCommittedGoRoutines.Load()))
		ok := p.AliveLock.TryLock()
		if ok {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	defer p.AliveLock.Unlock()
	p.Alive = false
	p.logger.Info("Closing partition", zap.String("partitionName", p.Name))
	close(p.LogInteractionRequests)
	return nil
}
