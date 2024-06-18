package client

import (
	"context"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

type PingPong struct {
	cancel  context.CancelFunc
	context context.Context
	logger  *zap.Logger
	stream  pb.Broker_PingPongClient
}

func (client *Client) NewPingPong() (*PingPong, error) {
	context, cancel := context.WithCancel(context.Background())
	stream, err := client.grpcClient.PingPong(context)
	if err != nil {
		client.logger.Error("Failed to issue ping pong", zap.Error(err))
		cancel()
		return nil, err
	}
	client.logger.Info("Issued ping pong call")
	p := &PingPong{
		cancel:  cancel,
		context: context,
		logger:  client.logger,
		stream:  stream,
	}
	return p, nil
}

func (pp *PingPong) PingPong() error {
	err := pp.stream.Send(&pb.Ping{})
	if err != nil {
		return err
	}
	_, err = pp.stream.Recv()
	return err
}

func (pp *PingPong) TimedPingPong() (time.Duration, error) {
	start := time.Now()
	err := pp.stream.Send(&pb.Ping{})
	if err != nil {
		return 0, err
	}
	_, err = pp.stream.Recv()
	end := time.Now()
	if err != nil {
		return 0, err
	}
	return end.Sub(start), nil
}

func (pp *PingPong) Close() error {
	pp.logger.Info("Finished ping pong call")
	pp.cancel()
	return nil
}
