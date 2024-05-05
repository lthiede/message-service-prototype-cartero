package main

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/lthiede/cartero/connection"
	"github.com/lthiede/cartero/messages"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()

	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(index int) {
			var swg sync.WaitGroup
			swg.Add(2)
			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				logger.Panic("Error starting connection", zap.Error(err))
			}
			defer conn.Close()
			go produce(index, conn, &swg, logger)
			go receiveAcks(index, conn, &swg, logger)
			swg.Wait()
			wg.Done()
		}(i)
	}
	wg.Wait()
	logger.Info("Client finished")
}

func produce(index int, conn net.Conn, wg *sync.WaitGroup, logger *zap.Logger) {
	defer wg.Done()
	logger.Info("Start producing", zap.Int("index", index))
	for i := 0; i < 5; i++ {
		err := produceRequest(index, conn, fmt.Sprintf("partition%d", i%3), uint64(i), []uint32{3, 100, 50, 26}, logger)
		if err != nil {
			logger.Error("Error sending produce request", zap.Error(err))
			return
		}
	}
	logger.Info("Finished producing", zap.Int("index", index))
}

func produceRequest(index int, conn net.Conn, partition string, batchId uint64, messageLengths []uint32, logger *zap.Logger) error {
	payloadLen := 0
	for _, l := range messageLengths {
		payloadLen += 4 + int(l)
	}
	partitionNameLen := 2 + len(partition)
	requestLengthEncodingLen := 4
	requestTypeEncodingLen := 1
	batchIdEncodingLen := 8
	var requestLen uint32 = uint32(requestTypeEncodingLen + partitionNameLen + batchIdEncodingLen + payloadLen)
	request := make([]byte, 0, int(requestLen)+requestLengthEncodingLen)
	request = binary.BigEndian.AppendUint32(request, uint32(requestLen))
	request = append(request, connection.RequestTypeProduce)
	request = binary.BigEndian.AppendUint16(request, uint16(len(partition)))
	request = append(request, []byte(partition)...)
	request = binary.BigEndian.AppendUint64(request, uint64(batchId))
	for message, messageLength := range messageLengths {
		request = binary.BigEndian.AppendUint32(request, messageLength)
		for j := 0; j < int(messageLength); j++ {
			request = append(request, byte(j*message*index))
		}
	}
	if logger.Level() == zap.DebugLevel {
		hasher := sha1.New()
		hasher.Write(request[4:])
		sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
		logger.Debug("Write request", zap.String("sha", sha), zap.ByteString("request", request))
	}
	n, err := conn.Write(request)
	if err != nil {
		return fmt.Errorf("error writing request to connection, wrote %d of %d bytes: %v", n, requestLen+uint32(requestLengthEncodingLen), err)
	}
	return nil
}

func receiveAcks(index int, conn net.Conn, wg *sync.WaitGroup, logger *zap.Logger) {
	defer wg.Done()
	logger.Info("Start receiving acks", zap.Int("index", index))
	for i := 0; i < 5; i++ {
		response, err := messages.ProtocolMessage(conn, logger)
		if err != nil {
			logger.Error("Error reading response", zap.Error(err))
			return
		}
		if response[0] == connection.ResponseTypeAckProduce {
			logger.Info("Received ack")
		} else {
			logger.Error("Received unrecognized response type")
			return
		}
		partition, bytesUsed, err := messages.NextString(response, logger)
		if err != nil {
			logger.Error("Error parsing partition name", zap.Error(err))
		}
		batchId, _, err := messages.NextUInt64(response[bytesUsed:])
		if err != nil {
			logger.Error("Error parsing batchId", zap.Error(err))
		}
		logger.Info("Received ack of batch", zap.Uint64("batchId", batchId), zap.String("partition", partition))
	}
	logger.Info("Finished receiving acks", zap.Int("Index", index))
}
