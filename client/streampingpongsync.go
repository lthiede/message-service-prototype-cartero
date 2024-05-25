package client

import (
	"context"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

type StreamPingPongSync struct {
	cancel  context.CancelFunc
	context context.Context
	logger  *zap.Logger
	stream  pb.Broker_StreamPingPongSyncClient
}

func (c *Client) NewStreamPingPongSync() (*StreamPingPongSync, error) {
	context, cancel := context.WithCancel(context.Background())
	stream, err := c.grpcClient.StreamPingPongSync(context)
	if err != nil {
		c.logger.Error("Failed to issue PingPong", zap.Error(err))
		cancel()
		return nil, err
	}
	pp := &StreamPingPongSync{
		cancel:  cancel,
		context: context,
		logger:  c.logger,
		stream:  stream,
	}
	return pp, nil
}

func (pp *StreamPingPongSync) PingPong() error {
	err := pp.stream.Send(&pb.Ping{})
	if err != nil {
		return err
	}
	_, err = pp.stream.Recv()
	return err
}

func (pp *StreamPingPongSync) TimedPingPong() (time.Duration, error) {
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

func (pp *StreamPingPongSync) Close() error {
	pp.cancel()
	pp.stream.CloseSend()
	return nil
}
