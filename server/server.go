package server

import (
	"fmt"
	"log"
	"net"

	"github.com/lthiede/cartero/connection"
	"github.com/lthiede/cartero/partition"
	"go.uber.org/zap"
)

type Server struct {
	partitions map[string]partition.Partition
	quit       chan int
	logger     *zap.Logger
}

func New(logger *zap.Logger) (*Server, error) {
	logger.Info("Creating new server")
	partitions := map[string]partition.Partition{}
	for i := 0; i <= 3; i++ {
		name := fmt.Sprintf("partition%d", i)
		p, err := partition.New(name, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating partition %s: %v", name, err)
		}
		go p.HandleProduce()
		partitions[name] = *p
	}
	return &Server{
		partitions,
		make(chan int),
		logger,
	}, nil
}

func (s *Server) ListenAndAccept() {
	l, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Println(err)
		return
	}
	s.logger.Info("Accepting connections on localhost:8080")
	defer l.Close()
	for {
		select {
		case <-s.quit:
			s.logger.Info("Stop accepting connections")
			return
		default:
			c, err := l.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			s.logger.Info("Accepted new connection")
			conn := connection.New(c, s.partitions, s.logger)
			go conn.HandleRequests()
			defer conn.Close()
		}
	}
}

func (s *Server) Close() error {
	s.logger.Debug("Closing server")
	for name, p := range s.partitions {
		err := p.Close()
		if err != nil {
			s.logger.Error("Error closing partition", zap.String("partition", name), zap.Error(err))
		}
	}
	close(s.quit)
	return nil
}
