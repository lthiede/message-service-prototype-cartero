package server

import (
	"fmt"
	"net"

	"github.com/lthiede/cartero/connection"
	"github.com/lthiede/cartero/partitionmanager"
	"go.uber.org/zap"
)

const PartitionNameMetadataKey string = "cartero.produce.partition_name"

type Server struct {
	addressListener  net.Listener
	partitionManager *partitionmanager.PartitionManager
	logger           *zap.Logger
	quit             chan struct{}
}

func New(partitionNames []string, address string, logger *zap.Logger) (*Server, error) {
	logger.Info("Creating new server")
	pm, err := partitionmanager.New(partitionNames, logger)
	if err != nil {
		return nil, fmt.Errorf("error trying to create partition manager: %v", err)
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("error trying to listen on %s: %v", address, err)
	}
	s := &Server{
		addressListener:  l,
		partitionManager: pm,
		logger:           logger,
		quit:             make(chan struct{}),
	}
	s.logger.Info("Accepting connections", zap.String("address", address))
	go s.acceptConnections()
	return s, nil
}

func (s *Server) acceptConnections() {
	for {
		select {
		case <-s.quit:
			s.logger.Info("Stop accepting connections")
			return
		default:
			c, err := s.addressListener.Accept()
			if err != nil {
				s.logger.Error("Failed to accept new connection", zap.Error(err))
				continue
			}
			s.logger.Info("Accepted new connection")
			conn, err := connection.New(c, s.partitionManager, s.logger)
			if err != nil {
				s.logger.Error("Failed to start handling new connection", zap.Error(err))
			}
			defer conn.Close()
		}
	}
}

func (s *Server) Close() error {
	s.logger.Debug("Closing server")
	close(s.quit)
	s.addressListener.Close()
	s.partitionManager.Close()
	return nil
}
