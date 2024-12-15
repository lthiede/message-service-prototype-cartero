package partition

import (
	"fmt"
	"math"
	"slices"
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
	ack                  *pb.ProduceAck
	produceResponse      chan *pb.Response
	alive                *bool
	aliveLock            *sync.RWMutex
	partitionReceiveTime time.Time
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

var MaxCheckLSNDelay = 200 * time.Microsecond

// TODO: try passing a batch of messages via cgo interface
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	loopLatenciesAppend := make([]time.Duration, 1000)
	loopLatenciesPoll := make([]time.Duration, 1000)
	appendLatencies := make([]time.Duration, 1000)
	pollLatencies := make([]time.Duration, 1000)
	var lsnAfterLastPolledCommittedLSN uint64
	var lsnAfterMostRecentLSN uint64
	checkScheduled := false
	for {
		startLoop := time.Now()
		lir, ok := <-p.LogInteractionRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			close(p.newCommittedLSN)
			close(p.outstandingAcks)
			slices.Sort(appendLatencies)
			slices.Sort(pollLatencies)
			slices.Sort(loopLatenciesAppend)
			slices.Sort(loopLatenciesPoll)
			p.logger.Info("Latencies",
				zap.Float64("appendLatencyP50", pct(appendLatencies, 0.5)),
				zap.Float64("appendLatencyP90", pct(appendLatencies, 0.9)),
				zap.Float64("appendLatencyP99", pct(appendLatencies, 0.99)),
				zap.Float64("appendLatencyP999", pct(appendLatencies, 0.999)),
				zap.Float64("appendLatencyP9999", pct(appendLatencies, 0.9999)),
				zap.Float64("pollLatencyP50", pct(pollLatencies, 0.5)),
				zap.Float64("pollLatencyP90", pct(pollLatencies, 0.9)),
				zap.Float64("pollLatencyP99", pct(pollLatencies, 0.99)),
				zap.Float64("pollLatencyP999", pct(pollLatencies, 0.999)),
				zap.Float64("pollLatencyP9999", pct(pollLatencies, 0.9999)),
				zap.Float64("loopLatencyPollP50", pct(loopLatenciesPoll, 0.5)),
				zap.Float64("loopLatencyPollP90", pct(loopLatenciesPoll, 0.9)),
				zap.Float64("loopLatencyPollP99", pct(loopLatenciesPoll, 0.99)),
				zap.Float64("loopLatencyPollP999", pct(loopLatenciesPoll, 0.999)),
				zap.Float64("loopLatencyPollP9999", pct(loopLatenciesPoll, 0.9999)),
				zap.Float64("loopLatencyAppendP50", pct(loopLatenciesAppend, 0.5)),
				zap.Float64("loopLatencyAppendP90", pct(loopLatenciesAppend, 0.9)),
				zap.Float64("loopLatencyAppendP99", pct(loopLatenciesAppend, 0.99)),
				zap.Float64("loopLatencyAppendP999", pct(loopLatenciesAppend, 0.999)),
				zap.Float64("loopLatencyAppendP9999", pct(loopLatenciesAppend, 0.9999)),
			)
			return
		}
		if abr := lir.AppendBatchRequest; abr != nil {
			numMessages := uint32(len(abr.EndOffsetsExclusively))
			p.outstandingAcks <- &outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       abr.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterMostRecentLSN,
					PartitionName: p.Name,
				},
				produceResponse:      abr.ProduceResponse,
				alive:                abr.Alive,
				aliveLock:            abr.AliveLock,
				partitionReceiveTime: time.Now(),
			}
			lsnAfterMostRecentLSN += uint64(numMessages)
			startAppend := time.Now()
			lsnAfterCommittedLSN, err := p.logClient.AppendAsync(abr.Payload, abr.EndOffsetsExclusively)
			appendLatencies = append(appendLatencies, time.Since(startAppend))
			if err != nil {
				p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			if lsnAfterCommittedLSN != lsnAfterMostRecentLSN && !checkScheduled {
				go func() {
					time.Sleep(MaxCheckLSNDelay)
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
				p.newCommittedLSN <- lsnAfterCommittedLSN
				lsnAfterLastPolledCommittedLSN = lsnAfterCommittedLSN
			}
			loopLatenciesAppend = append(loopLatenciesAppend, time.Since(startLoop))
		} else if lir.pollCommittedRequest != nil {
			checkScheduled = false
			startPoll := time.Now()
			committedLSN, err := p.logClient.PollCompletion()
			pollLatencies = append(pollLatencies, time.Since(startPoll))
			if err != nil {
				p.logger.Error("Error polling committed LSN", zap.Error(err), zap.String("partitionName", p.Name))
			}
			if err != nil || committedLSN+1 < lsnAfterMostRecentLSN {
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
				p.newCommittedLSN <- committedLSN + 1
				lsnAfterLastPolledCommittedLSN = committedLSN + 1
			}
			loopLatenciesPoll = append(loopLatenciesPoll, time.Since(startLoop))
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
	partitionLatenciesExludingSendAck := make([]time.Duration, 0)
	partitionLatenciesIncludingSendAck := make([]time.Duration, 0)
	for {
		lsnAfterCommittedLSN, ok := <-p.newCommittedLSN
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
			p.nextLSNCommitted.Store(math.MaxUint64)
			slices.Sort(partitionLatenciesExludingSendAck)
			slices.Sort(partitionLatenciesIncludingSendAck)
			p.logger.Info("Latencies",
				zap.Float64("partitionLatencyExludingSendAckP50", pct(partitionLatenciesExludingSendAck, 0.5)),
				zap.Float64("partitionLatencyExludingSendAckP90", pct(partitionLatenciesExludingSendAck, 0.9)),
				zap.Float64("partitionLatencyExludingSendAckP99", pct(partitionLatenciesExludingSendAck, 0.99)),
				zap.Float64("partitionLatencyExludingSendAckP999", pct(partitionLatenciesExludingSendAck, 0.999)),
				zap.Float64("partitionLatencyExludingSendAckP9999", pct(partitionLatenciesExludingSendAck, 0.9999)),
				zap.Float64("partitionLatencyIncludingSendAckP50", pct(partitionLatenciesIncludingSendAck, 0.5)),
				zap.Float64("partitionLatencyIncludingSendAckP90", pct(partitionLatenciesIncludingSendAck, 0.9)),
				zap.Float64("partitionLatencyIncludingSendAckP99", pct(partitionLatenciesIncludingSendAck, 0.99)),
				zap.Float64("partitionLatencyIncludingSendAckP999", pct(partitionLatenciesIncludingSendAck, 0.999)),
				zap.Float64("partitionLatencyIncludingSendAckP9999", pct(partitionLatenciesIncludingSendAck, 0.9999)),
			)
			return
		}
		p.nextLSNCommitted.Store(lsnAfterCommittedLSN)
		if longestOutstandingAck != nil {
			if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
				partitionLatenciesExludingSendAck = append(partitionLatenciesExludingSendAck, time.Since(longestOutstandingAck.partitionReceiveTime))
				longestOutstandingAck.aliveLock.RLock()
				if *longestOutstandingAck.alive {
					longestOutstandingAck.produceResponse <- &pb.Response{
						Response: &pb.Response_ProduceAck{
							ProduceAck: longestOutstandingAck.ack,
						},
					}
					partitionLatenciesIncludingSendAck = append(partitionLatenciesExludingSendAck, time.Since(longestOutstandingAck.partitionReceiveTime))
				}
				longestOutstandingAck.aliveLock.RUnlock()
			} else {
				continue
			}
		}
	moreAcks:
		for {
			select {
			case longestOutstandingAck = <-p.outstandingAcks:
				if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
					partitionLatenciesExludingSendAck = append(partitionLatenciesExludingSendAck, time.Since(longestOutstandingAck.partitionReceiveTime))
					longestOutstandingAck.aliveLock.RLock()
					if *longestOutstandingAck.alive {
						longestOutstandingAck.produceResponse <- &pb.Response{
							Response: &pb.Response_ProduceAck{
								ProduceAck: longestOutstandingAck.ack,
							},
						}
						partitionLatenciesIncludingSendAck = append(partitionLatenciesExludingSendAck, time.Since(longestOutstandingAck.partitionReceiveTime))
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
	p.AliveLock.Lock()
	defer p.AliveLock.Unlock()
	p.Alive = false
	p.logger.Info("Closing partition", zap.String("partitionName", p.Name))
	close(p.LogInteractionRequests)
	return nil
}
