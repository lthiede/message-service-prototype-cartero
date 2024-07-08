package client

import (
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Client struct {
	logger                     *zap.Logger
	conn                       net.Conn
	producers                  map[string]*Producer
	producersRWMutex           sync.RWMutex
	pingPongs                  map[string]*PingPong
	pingPongsRWMutex           sync.RWMutex
	consumers                  map[string]*Consumer
	consumersRWMutex           sync.RWMutex
	expectedCreatePartitionRes map[string]chan bool
	objectStorageClient        *minio.Client
	done                       chan struct{}
}

func minioClient() (*minio.Client, error) {
	endpoint := "127.0.0.1:9000"

	// Initialize minio client object.
	options := &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	}
	return minio.New(endpoint, options)
}

func New(address string, logger *zap.Logger) (*Client, error) {
	return NewWithOptions(address, "" /*localAddr*/, logger)
}

func NewWithOptions(address string, localAddr string, logger *zap.Logger) (*Client, error) {
	dialer := &net.Dialer{}
	if localAddr != "" {
		dialer.LocalAddr = &net.TCPAddr{
			IP:   net.ParseIP(localAddr),
			Port: 0,
		}
		logger.Info("Using local address", zap.String("localAddress", localAddr))
	}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}
	logger.Info("Dialed server", zap.String("address", address))
	client := &Client{
		logger:                     logger,
		conn:                       conn,
		producers:                  map[string]*Producer{},
		pingPongs:                  map[string]*PingPong{},
		consumers:                  map[string]*Consumer{},
		expectedCreatePartitionRes: map[string]chan bool{},
		done:                       make(chan struct{}),
	}
	go client.handleResponses()
	return client, nil
}

func (c *Client) handleResponses() {
	for {
		select {
		case <-c.done:
			c.logger.Info("Stop handling responses")
			return
		default:
			response := &pb.Response{}
			err := protodelim.UnmarshalFrom(&readertobytereader.ReaderByteReader{Reader: c.conn}, response)
			if err != nil {
				c.logger.Error("Failed to unmarshal response", zap.Error(err))
				return
			}
			switch res := response.Response.(type) {
			case *pb.Response_ProduceAck:
				produceAck := res.ProduceAck
				c.producersRWMutex.RLock()
				p, ok := c.producers[produceAck.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", produceAck.PartitionName))
					continue
				}
				c.logger.Info("Received ack", zap.Uint64("batchId", produceAck.BatchId), zap.Uint64("numberMessages", produceAck.EndOffset-produceAck.StartOffset))
				p.UpdateAcknowledged(produceAck.BatchId, produceAck.EndOffset-produceAck.StartOffset)
				c.producersRWMutex.RUnlock()
			case *pb.Response_PingPongResponse:
				pingPongRes := res.PingPongResponse
				c.pingPongsRWMutex.RLock()
				pp, ok := c.pingPongs[pingPongRes.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", pingPongRes.PartitionName))
					continue
				}
				pp.Responses <- struct{}{}
				c.pingPongsRWMutex.RUnlock()
			case *pb.Response_ConsumeResponse:
				consumeRes := res.ConsumeResponse
				c.consumersRWMutex.RLock()
				cons, ok := c.consumers[consumeRes.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", consumeRes.PartitionName))
					continue
				}
				c.logger.Info("Client received safe consume offset", zap.String("partitionName", consumeRes.PartitionName), zap.Int("offset", int(consumeRes.EndOfSafeOffsetsExclusively)))
				cons.UpdateEndOfSafeOffsetsExclusively(consumeRes.EndOfSafeOffsetsExclusively)
				c.consumersRWMutex.RUnlock()
			case *pb.Response_LogConsumeResponse:
				logConsumeRes := res.LogConsumeResponse
				if !logConsumeRes.RedirectS3 {
					c.logger.Error("Received log consume response that doesn't redirect to s3. This isn't supported yet", zap.String("partitionName", logConsumeRes.PartitionName))
				}
				c.consumersRWMutex.RLock()
				cons, ok := c.consumers[logConsumeRes.PartitionName]
				if !ok {
					c.logger.Error("Partition not recognized", zap.String("partitionName", logConsumeRes.PartitionName))
					continue
				}
				c.logger.Info("Client received s3 objects to read", zap.String("partitionName", logConsumeRes.PartitionName), zap.Strings("s3Objects", logConsumeRes.ObjectNames))
				cons.NewS3ObjectNames <- logConsumeRes.ObjectNames
				c.consumersRWMutex.RUnlock()
			case *pb.Response_CreatePartitionResponse:
				createPartitionRes := res.CreatePartitionResponse
				successChan, ok := c.expectedCreatePartitionRes[createPartitionRes.PartitionName]
				if !ok {
					c.logger.Error("Received a create partition response but not waiting for one", zap.String("partitionName", createPartitionRes.PartitionName))
					continue
				}
				successChan <- createPartitionRes.Successful
			default:
				c.logger.Error("Request type not recognized")
			}
		}
	}
}

func (c *Client) Close() error {
	moreImportantErr := c.conn.Close()
	if moreImportantErr != nil {
		c.logger.Error("Failed to close connection", zap.Error(moreImportantErr))
	}
	lessImportantErr := c.logger.Sync()
	if lessImportantErr != nil {
		log.Printf("Failed to sync logger: %v", lessImportantErr)
	}
	if moreImportantErr != nil {
		return moreImportantErr
	} else {
		return lessImportantErr
	}
}
