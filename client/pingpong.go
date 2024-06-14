package client

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type PingPong struct {
	client        *Client
	conn          net.Conn
	logger        *zap.Logger
	partitionName string
	numPingPongs  atomic.Uint64
}

func (client *Client) NewPingPong(partitionName string) (*PingPong, error) {
	client.pingPongsRWMutex.Lock()
	pp, ok := client.pingPongs[partitionName]
	if ok {
		return pp, nil
	}
	pp = &PingPong{
		client:        client,
		conn:          client.conn,
		logger:        client.logger,
		partitionName: partitionName,
	}
	client.pingPongs[partitionName] = pp
	client.pingPongsRWMutex.Unlock()
	return pp, nil
}

func (pp *PingPong) SendPingPong() error {
	oldNumPingPongs := pp.numPingPongs.Load()
	req := &pb.Request{
		Request: &pb.Request_PingPongRequest{
			PingPongRequest: &pb.PingPongRequest{
				PartitionName: pp.partitionName,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}
	_, err = pp.conn.Write(wireMessage.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send ping pong request: %v", err)
	}
	for pp.numPingPongs.Load() < oldNumPingPongs+1 {

	}
	return nil
}

func (pp *PingPong) SendTimedPingPong() (time.Duration, error) {
	start := time.Now()
	oldNumPingPongs := pp.numPingPongs.Load()
	req := &pb.Request{
		Request: &pb.Request_PingPongRequest{
			PingPongRequest: &pb.PingPongRequest{
				PartitionName: pp.partitionName,
			},
		},
	}
	wireMessage := &bytes.Buffer{}
	_, err := protodelim.MarshalTo(wireMessage, req)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message: %v", err)
	}
	_, err = pp.conn.Write(wireMessage.Bytes())
	if err != nil {
		return 0, fmt.Errorf("failed to send ping pong request: %v", err)
	}
	for pp.numPingPongs.Load() < oldNumPingPongs+1 {

	}
	end := time.Now()
	return end.Sub(start), nil
}

func (pp *PingPong) UpdateNumPingPongs() {
	oldNumPingPongs := pp.numPingPongs.Load()
	pp.numPingPongs.Store(oldNumPingPongs + 1)
}

func (pp *PingPong) Close() error {
	pp.logger.Info("Finished ping pong", zap.String("partitionName", pp.partitionName))

	pp.client.pingPongsRWMutex.Lock()
	delete(pp.client.pingPongs, pp.partitionName)
	pp.client.pingPongsRWMutex.Unlock()

	return nil
}
