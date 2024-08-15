package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"github.com/lthiede/cartero/readertobytereader"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/v2/certs"
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
	consumers                  map[string]GeneralConsumer
	consumersRWMutex           sync.RWMutex
	expectedCreatePartitionRes map[string]chan bool
	objectStorageClient        *minio.Client
	minioAddress               string
	done                       chan struct{}
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	rootCAs, err := certs.GetRootCAs("")
	if err != nil {
		rootCAs, err = x509.SystemCertPool()
		if err != nil {
			return x509.NewCertPool()
		}
	}
	return rootCAs
}

func clientTransport(concurrency int) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   concurrency,
		WriteBufferSize:       32 * 1024, // 32KiB up from 4KiB default
		ReadBufferSize:        32 * 1024, // 32KiB up from 4KiB default
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
		DisableKeepAlives:  false,
		TLSClientConfig: &tls.Config{
			RootCAs:            mustGetSystemCertPool(),
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false,
		},
	}
}

func MinioClient(address string, accessKey string, secretAccessKey string, concurrency int) (*minio.Client, error) {
	// Initialize minio client object.
	options := &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretAccessKey, ""),
		Secure:    true,
		Transport: clientTransport(concurrency),
	}
	return minio.New(address, options)
}

func New(address string, minioAddress string, s3AccessKey string, s3SecretAccessKey string, logger *zap.Logger) (*Client, error) {
	return NewWithOptions(address, minioAddress, s3AccessKey, s3SecretAccessKey, "" /*localAddr*/, logger)
}

func NewWithOptions(address string, minioAddress string, s3AccessKey string, s3SecretAccessKey string, localAddr string, logger *zap.Logger) (*Client, error) {
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
	objectStorageClient, err := MinioClient(minioAddress, s3AccessKey, s3SecretAccessKey, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to make minio client: %v", err)
	}
	logger.Info("Created object storage client")
	client := &Client{
		logger:                     logger,
		conn:                       conn,
		producers:                  map[string]*Producer{},
		pingPongs:                  map[string]*PingPong{},
		consumers:                  map[string]GeneralConsumer{},
		expectedCreatePartitionRes: map[string]chan bool{},
		minioAddress:               minioAddress,
		objectStorageClient:        objectStorageClient,
		done:                       make(chan struct{}),
	}
	go client.handleResponses()
	return client, nil
}

type GeneralConsumer interface {
	UpdateEndOfSafeOffsetsExclusively(endOfSafeOffsetsExclusively uint64)
	FeedNewS3ObjectNames(s3ObjectNames []string)
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
				// c.logger.Info("Client received s3 objects to read", zap.String("partitionName", logConsumeRes.PartitionName), zap.Strings("s3Objects", logConsumeRes.ObjectNames))
				cons.FeedNewS3ObjectNames(logConsumeRes.ObjectNames)
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
