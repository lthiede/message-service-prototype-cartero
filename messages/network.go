package messages

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"go.uber.org/zap"
)

func ProtocolMessage(reader io.Reader, logger *zap.Logger) ([]byte, error) {
	protocolMessageLength, err := protocolMessageLength(reader, logger)
	if err != nil {
		return nil, fmt.Errorf("couldn't read request length: %v", err)
	}
	logger.Debug("Reading request of length", zap.Uint32("requestLength", protocolMessageLength))
	protocolMessage := make([]byte, protocolMessageLength)
	for i := 0; i < int(protocolMessageLength); {
		b := protocolMessage[i:]
		n, err := reader.Read(b)
		if err != nil {
			if n+i != int(protocolMessageLength) {
				return nil, fmt.Errorf("couldn't read %d bytes containing request, read %d bytes: %v", protocolMessageLength, n+i, err)
			}
			logger.Error("Error reading request", zap.Error(err))
		}
		i += n
	}
	if logger.Level() == zap.DebugLevel {
		hasher := sha1.New()
		hasher.Write(protocolMessage)
		sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
		logger.Debug("Read request", zap.String("sha", sha), zap.ByteString("request", protocolMessage))
	}
	return protocolMessage, nil
}

func protocolMessageLength(reader io.Reader, logger *zap.Logger) (uint32, error) {
	protocolMessageLengthBytes := make([]byte, 4)
	for i := 0; i < 4; {
		b := protocolMessageLengthBytes[i:]
		n, err := reader.Read(b)
		if err != nil {
			if n+i != 4 {
				return 0, fmt.Errorf("couldn't read 4 bytes encoding request length, read %d bytes: %v", n+i, err)
			}
			logger.Error("Error reading bytes encoding request length", zap.Error(err))
		}
		i += n
	}

	var protocolMessageLength uint32
	err := binary.Read(bytes.NewReader(protocolMessageLengthBytes), binary.BigEndian, &protocolMessageLength)
	if err != nil {
		return 0, fmt.Errorf("couldn't encode request length: %v", err)
	}
	return protocolMessageLength, nil
}

func NextString(protocolMessage []byte, logger *zap.Logger) (string, int, error) {
	var stringLength uint16
	err := binary.Read(bytes.NewReader(protocolMessage[:2]), binary.BigEndian, &stringLength)
	if err != nil {
		return "", 0, fmt.Errorf("error reading length of string: %v", err)
	}
	logger.Debug("Reading string of length", zap.Uint16("stringLength", stringLength))
	endOfString := stringLength + 2
	return string(protocolMessage[2:endOfString]), int(endOfString), nil
}

func NextUInt64(protocolMessage []byte) (uint64, int, error) {
	var longInt uint64
	err := binary.Read(bytes.NewReader(protocolMessage), binary.BigEndian, &longInt)
	if err != nil {
		return 0, 0, fmt.Errorf("error reading int: %v", err)
	}
	return longInt, 8, nil
}
