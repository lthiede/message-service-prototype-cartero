package partition

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"

	logclient "github.com/toziegler/rust-segmentstore/libsls-bindings/go_example/client"

	"go.uber.org/zap"
)

const MaxMessageSize = 3800

type Partition struct {
	Name                   string
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

// TODO: try passing a batch of messages via cgo interface
// Don't add a channel select to this function
// Receiving on the channel is not a bottleneck
func (p *Partition) logInteractions() {
	p.logger.Info("Start handling produce", zap.String("partitionName", p.Name))
	var lsnAfterLastPolledCommittedLSN uint64
	var lsnAfterMostRecentLSN uint64
	passNewCommittedDuration := make([]time.Duration, 0)
	passOutstandingDuration := make([]time.Duration, 0)
	getAppendBatchRequestDuration := make([]time.Duration, 0)
	getPollCommittedRequestDuration := make([]time.Duration, 0)
	pollCommittedDuration := make([]time.Duration, 0)
	appendBatchDuration := make([]time.Duration, 0)
	checkScheduled := false
	for {
		startLogInteractionRequest := time.Now()
		lir, ok := <-p.LogInteractionRequests
		if !ok {
			p.logger.Info("Stop handling produce", zap.String("partitionName", p.Name))
			close(p.newCommittedLSN)
			close(p.outstandingAcks)
			p.logger.Info("Measured durations",
				zap.Float64("getAppendBatchRequestDuration50Pct", pct(getAppendBatchRequestDuration, 0.5)),
				zap.Float64("getPollCommittedRequestDuration50Pct", pct(getPollCommittedRequestDuration, 0.5)),
				zap.Float64("passNewCommittedDuration50pct", pct(passNewCommittedDuration, 0.5)),
				zap.Float64("passOutstandingDuration50pct", pct(passOutstandingDuration, 0.5)),
				zap.Float64("pollCommittedDuration50pct", pct(pollCommittedDuration, 0.5)),
				zap.Float64("appendBatchDuration50pct", pct(appendBatchDuration, 0.5)),
			)
			return
		}
		if abr := lir.AppendBatchRequest; abr != nil {
			getAppendBatchRequestDuration = append(getAppendBatchRequestDuration, time.Since(startLogInteractionRequest))
			numMessages := uint32(len(abr.EndOffsetsExclusively))
			startPassOutstanding := time.Now()
			p.outstandingAcks <- &outstandingAck{
				ack: &pb.ProduceAck{
					BatchId:       abr.BatchId,
					NumMessages:   numMessages,
					StartLsn:      lsnAfterMostRecentLSN,
					PartitionName: p.Name,
				},
				produceResponse: abr.ProduceResponse,
			}
			passOutstandingDuration = append(passOutstandingDuration, time.Since(startPassOutstanding))
			lsnAfterMostRecentLSN += uint64(numMessages)
			startAppendBatch := time.Now()
			lsnAfterCommittedLSN, err := p.logClient.AppendAsync(abr.Payload, abr.EndOffsetsExclusively)
			appendBatchDuration = append(appendBatchDuration, time.Since(startAppendBatch))
			if err != nil {
				p.logger.Error("Error appending batch to log", zap.Error(err), zap.String("partitionName", p.Name))
				return
			}
			if lsnAfterCommittedLSN != lsnAfterMostRecentLSN && !checkScheduled {
				go p.sendPollCommittedRequest()
				checkScheduled = true
			}
			if lsnAfterCommittedLSN != 0 && lsnAfterCommittedLSN != lsnAfterLastPolledCommittedLSN {
				startPassNewCommitted := time.Now()
				p.newCommittedLSN <- lsnAfterCommittedLSN
				passNewCommittedDuration = append(passNewCommittedDuration, time.Since(startPassNewCommitted))
				lsnAfterLastPolledCommittedLSN = lsnAfterCommittedLSN
			}
		} else if lir.pollCommittedRequest != nil {
			getPollCommittedRequestDuration = append(getPollCommittedRequestDuration, time.Since(startLogInteractionRequest))
			checkScheduled = false
			startPollCommittedDuration := time.Now()
			committedLSN, err := p.logClient.PollCompletion()
			pollCommittedDuration = append(pollCommittedDuration, time.Since(startPollCommittedDuration))
			if err != nil {
				p.logger.Error("Error polling committed LSN", zap.Error(err), zap.String("partitionName", p.Name))
			}
			if err != nil || committedLSN+1 < lsnAfterMostRecentLSN {
				go p.sendPollCommittedRequest()
				checkScheduled = true
			}
			if err == nil && committedLSN+1 != lsnAfterLastPolledCommittedLSN {
				startPassNewCommitted := time.Now()
				p.newCommittedLSN <- committedLSN + 1
				passNewCommittedDuration = append(passNewCommittedDuration, time.Since(startPassNewCommitted))
				lsnAfterLastPolledCommittedLSN = committedLSN + 1
			}
		}
	}
}

func (p *Partition) sendPollCommittedRequest() {
	defer func() {
		if err := recover(); err != nil {
			p.logger.Error("Caught error", zap.Any("err", err))
		}
	}()
	p.LogInteractionRequests <- LogInteractionRequest{
		pollCommittedRequest: &struct{}{},
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
		lsnAfterCommittedLSN, ok := <-p.newCommittedLSN
		if !ok {
			p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
			p.nextLSNCommitted.Store(math.MaxUint64)
			return
		}
		p.nextLSNCommitted.Store(lsnAfterCommittedLSN)
		if longestOutstandingAck != nil {
			if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
				p.sendAck(longestOutstandingAck)
			} else {
				continue
			}
		}
	moreAcks:
		for {
			select {
			case longestOutstandingAck, ok = <-p.outstandingAcks:
				if !ok {
					p.logger.Info("Stop handling acks", zap.String("partitionName", p.Name))
					p.nextLSNCommitted.Store(math.MaxUint64)
					return
				}
				if lsnAfterCommittedLSN >= longestOutstandingAck.ack.StartLsn+uint64(longestOutstandingAck.ack.NumMessages) {
					p.sendAck(longestOutstandingAck)
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

func (p *Partition) sendAck(ack *outstandingAck) {
	defer func() {
		if err := recover(); err != nil {
			p.logger.Error("Caught error", zap.Any("err", err))
		}
	}()
	ack.produceResponse <- &pb.Response{
		Response: &pb.Response_ProduceAck{
			ProduceAck: ack.ack,
		},
	}
}

func (p *Partition) NextLSN() uint64 {
	return p.nextLSNCommitted.Load()
}

func (p *Partition) Close() error {
	p.logger.Info("Closing partition", zap.String("partitionName", p.Name))
	close(p.LogInteractionRequests)
	return nil
}
