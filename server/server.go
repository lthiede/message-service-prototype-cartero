package server

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/lthiede/cartero/cache"
	"github.com/lthiede/cartero/partition"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const PartitionNameMetadataKey string = "cartero.produce.partition_name"

// TODO: make sure everything is closed correctly
type Server struct {
	pb.UnimplementedBrokerServer
	logger     *zap.Logger
	grpcServer *grpc.Server
	partitions map[string]partition.Partition
	cache      *cache.Cache
}

func New(partitionNames []string, address string, logger *zap.Logger) (*Server, error) {
	logger.Info("Creating new server")
	cache := cache.New(partitionNames, logger)
	partitions := map[string]partition.Partition{}
	for _, partitionName := range partitionNames {
		p, err := partition.New(partitionName, cache, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating partition %s: %v", partitionName, err)
		}
		partitions[partitionName] = *p
	}
	s := &Server{
		logger:     logger,
		grpcServer: grpc.NewServer(),
		partitions: partitions,
		cache:      cache,
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("error trying to listen on %s: %v", address, zap.Error(err))
	}
	s.logger.Info("Accepting connections", zap.String("address", address))
	pb.RegisterBrokerServer(s.grpcServer, s)
	go s.grpcServer.Serve(l)
	return s, nil
}

func (s *Server) partitionFromMetadata(stream grpc.ServerStream) (*partition.Partition, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "metadata missing")
	}
	v, ok := md[PartitionNameMetadataKey]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "metadata value for key cartero.produce.partition_name missing")
	}
	if len(v) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "too many metadata values for key cartero.produce.partition_name, expected 1, got %d", len(v))
	}
	partitionName := v[0]
	p, ok := s.partitions[partitionName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "partition %s doesn't exist", partitionName)
	}
	return &p, nil
}

func (s *Server) Produce(stream pb.Broker_ProduceServer) error {
	s.logger.Info("Handling produce request")
	p, err := s.partitionFromMetadata(stream)
	if err != nil {
		return err
	}
	s.logger.Info("Handling production of stream of message batches", zap.String("partitionName", p.Name))
	acks := make(chan *pb.ProduceAck)
	handleIncomingBatchesErr := make(chan error)
	go s.handleIncomingBatches(stream, p, acks, handleIncomingBatchesErr)
	// this part of the code is only allowed to call Send, not Recv
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ack := <-acks:
			err = stream.Send(ack)
			if err != nil {
				return err
			}
			s.logger.Info("Acknowledged batch", zap.String("partitionName", p.Name), zap.Int("batchId", int(ack.BatchId)), zap.Int("startOffset", int(ack.StartOffset)), zap.Int("endOffset", int(ack.EndOffset)))
		case incomingBatchError := <-handleIncomingBatchesErr:
			if incomingBatchError != nil {
				return incomingBatchError
			}
		}
	}
}

// This method is only allowed to call Recv, not Send
func (s *Server) handleIncomingBatches(stream pb.Broker_ProduceServer, p *partition.Partition, acks chan *pb.ProduceAck, returnErr chan<- error) {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			returnErr <- err
			return
		}
		if in.BatchId == 0 {
			s.logger.Warn("Received default value of BatchId 0. This could indicate missing value")
		}
		p.ProduceRequests <- partition.ProduceRequest{
			BatchId:  in.BatchId,
			Messages: in.Messages,
			Ack:      acks,
		}
	}
}

func (s *Server) Consume(stream pb.Broker_ConsumeServer) error {
	s.logger.Info("Handling consume request")
	p, err := s.partitionFromMetadata(stream)
	if err != nil {
		return err
	}
	s.logger.Info("Handling consumption of stream of message batches", zap.String("partitionName", p.Name))
	cacheConsumer := &cache.Consumer{
		Messages: make(chan *pb.ConsumeMessageBatch),
		Quit:     make(chan struct{}),
	}
	defer close(cacheConsumer.Quit)
	handleIncomingConsumeRequestsErr := make(chan error)
	go s.handleIncomingConsumeRequests(stream, p, cacheConsumer, handleIncomingConsumeRequestsErr)
	// this part of the code is only allowed to call Send, not Recv
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case messageBatch := <-cacheConsumer.Messages:
			consumeResponse := &pb.ConsumeResponse{
				ConsumeMessageBatch: messageBatch,
			}
			err = stream.Send(consumeResponse)
			if err != nil {
				return err
			}
		case incomingBatchError := <-handleIncomingConsumeRequestsErr:
			if incomingBatchError != nil {
				return incomingBatchError
			}
		}
	}
}

// This method is only allowed to call Recv, not Send
func (s *Server) handleIncomingConsumeRequests(stream pb.Broker_ConsumeServer, p *partition.Partition, cacheConsumer *cache.Consumer, returnErr chan<- error) {
	alreadyReadingFromCache := false
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			returnErr <- err
			return
		}
		if alreadyReadingFromCache {
			s.logger.DPanic("Already handling a consume request")
			close(cacheConsumer.Quit)
		}
		cacheConsumer.StartOffset = int(in.StartOffset)
		cacheReadErr := s.cache.StartReading(cacheConsumer, p.Name)
		if cacheReadErr != nil && cacheReadErr.ReadFromLog {
			redirectResponse := &pb.ConsumeResponse{
				IsRedirectResponse: true,
				RedirectResponse: &pb.RedirectResponse{
					SaveEndOffset: p.Offset,
				},
			}
			err = stream.Send(redirectResponse)
			if err != nil {
				returnErr <- err
				return
			}
			continue
		}
		if cacheReadErr != nil {
			returnErr <- status.Error(codes.InvalidArgument, "partition doesn't have cached data")
			return
		}
		alreadyReadingFromCache = true
	}
}

func (s *Server) PingPong(ctx context.Context, in *pb.Ping) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}

func (s *Server) Close() error {
	s.logger.Debug("Closing server")
	s.grpcServer.GracefulStop()
	for name, p := range s.partitions {
		err := p.Close()
		if err != nil {
			s.logger.Error("Error closing partition", zap.String("partitionName", name), zap.Error(err))
		}
	}
	return s.cache.Close()
}
