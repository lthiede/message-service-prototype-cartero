package client

import (
	"context"
	"time"

	pb "github.com/lthiede/cartero/proto"
)

type UnaryPingPongSync struct {
	client pb.BrokerClient
}

func (c *Client) NewUnaryPingPongSync() (*UnaryPingPongSync, error) {
	return &UnaryPingPongSync{
		client: c.grpcClient,
	}, nil
}

func (pp *UnaryPingPongSync) PingPong() error {
	_, err := pp.client.UnaryPingPongSync(context.Background(), &pb.Ping{})
	return err
}

func (pp *UnaryPingPongSync) TimedPingPong() (time.Duration, error) {
	start := time.Now()
	_, err := pp.client.UnaryPingPongSync(context.Background(), &pb.Ping{})
	end := time.Now()
	if err != nil {
		return 0, err
	}
	return end.Sub(start), nil
}

func (pp *UnaryPingPongSync) Close() error {
	return nil
}
