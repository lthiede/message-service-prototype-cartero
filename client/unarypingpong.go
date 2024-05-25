package client

import (
	"context"
	"time"

	pb "github.com/lthiede/cartero/proto"
)

type UnaryPingPong struct {
	client pb.BrokerClient
}

func (c *Client) NewUnaryPingPong() (*UnaryPingPong, error) {
	return &UnaryPingPong{
		client: c.grpcClient,
	}, nil
}

func (pp *UnaryPingPong) PingPong() error {
	_, err := pp.client.UnaryPingPong(context.Background(), &pb.Ping{})
	return err
}

func (pp *UnaryPingPong) TimedPingPong() (time.Duration, error) {
	start := time.Now()
	_, err := pp.client.UnaryPingPong(context.Background(), &pb.Ping{})
	end := time.Now()
	if err != nil {
		return 0, err
	}
	return end.Sub(start), nil
}

func (pp *UnaryPingPong) Close() error {
	return nil
}
