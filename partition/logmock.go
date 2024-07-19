package partition

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	pb "github.com/lthiede/cartero/proto"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

type logMock struct {
	name                   string
	logFileLock            sync.Mutex
	currentFile            *os.File
	prevNextFile           *os.File
	numMessagesTotal       uint64
	startOffsetCurrentFile uint64
	objectStorageClient    *minio.Client
	archivedFiles          []uint64
	s3ObjectNameRequests   chan s3ObjectNameRequest
	logger                 *zap.Logger
	quit                   chan struct{}
}

type s3ObjectNameRequest struct {
	startOffset          uint64
	endOffsetExclusively uint64
	response             chan []string
}

func NewLogMock(objectStorageClient *minio.Client, name string, logger *zap.Logger) (*logMock, error) {
	bucketExists, err := objectStorageClient.BucketExists(context.TODO(), name)
	if err != nil {
		return nil, fmt.Errorf("error checking if bucket already exists: %v", err)
	}
	if bucketExists {
		logger.Warn("Bucket already exists. It might contain old data", zap.String("partitionName", name))
	} else {
		err := objectStorageClient.MakeBucket(context.TODO(), name, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("error creating bucket: %v", err)
		}
		logger.Debug("Created bucket", zap.String("partitionName", name))
	}
	path := filepath.Join(".", "data")
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("error creating storage directory: %v", err)
	}
	currentFile, err := os.CreateTemp(path, name)
	if err != nil {
		return nil, fmt.Errorf("error creating the storage file: %v", err)
	}
	prevNextFile, err := os.CreateTemp(path, name)
	if err != nil {
		return nil, fmt.Errorf("error creating the storage file: %v", err)
	}
	logger.Debug("Created files", zap.String("partitionName", name), zap.String("file1", currentFile.Name()), zap.String("file2", prevNextFile.Name()))
	lm := &logMock{
		name:                 name,
		currentFile:          currentFile,
		prevNextFile:         prevNextFile,
		objectStorageClient:  objectStorageClient,
		archivedFiles:        make([]uint64, 0),
		s3ObjectNameRequests: make(chan s3ObjectNameRequest),
		logger:               logger,
		quit:                 make(chan struct{}),
	}
	go lm.synchedLoop()
	return lm, nil
}

type DebugWriter struct {
	io.Writer
}

func (dw *DebugWriter) Write(p []byte) (n int, err error) {
	fmt.Println(p)
	return dw.Writer.Write(p)
}

func (lm *logMock) Persist(messages *pb.Messages) error {
	lm.logFileLock.Lock()
	_, err := protodelim.MarshalTo(lm.currentFile, messages)
	if err != nil {
		lm.logFileLock.Unlock()
		return fmt.Errorf("failed to write batch to file: %v", err)
	}
	lm.numMessagesTotal += uint64(len(messages.Messages))
	lm.logFileLock.Unlock()
	return nil
}

var waitToPersist time.Duration = 8500 * time.Millisecond

func (lm *logMock) synchedLoop() {
	lm.logger.Info("Start archiving log files to object storage", zap.String("partitionName", lm.name))
	upload := make(chan struct{})
	go func() {
		time.Sleep(waitToPersist)
		upload <- struct{}{}
	}()
	for {
		select {
		case <-lm.quit:
			lm.logger.Info("Stop archiving log files to object storage", zap.String("partitionName", lm.name))
			return
		case <-upload:
			lm.uploadToS3()
			go func() {
				time.Sleep(waitToPersist)
				upload <- struct{}{}
			}()
		case req := <-lm.s3ObjectNameRequests:
			req.response <- lm.s3ObjectNames(req.startOffset, req.endOffsetExclusively)
		}
	}
}

type DebugReader struct {
	io.Reader
}

func (dr *DebugReader) Read(b []byte) (n int, err error) {
	n, err = dr.Reader.Read(b)
	fmt.Println(n)
	fmt.Println(b)
	return
}

func (lm *logMock) uploadToS3() {
	fileToUpload := lm.currentFile
	lm.logFileLock.Lock()
	if lm.numMessagesTotal == lm.startOffsetCurrentFile {
		lm.logFileLock.Unlock()
		return
	}
	startOffset := lm.startOffsetCurrentFile
	lm.startOffsetCurrentFile = lm.numMessagesTotal
	lm.currentFile = lm.prevNextFile
	lm.logFileLock.Unlock()
	// upload fileToUpload
	fileStats, err := fileToUpload.Stat()
	if err != nil {
		lm.logger.Fatal("Failed to get file stats", zap.String("partitionName", lm.name), zap.Error(err))
	}
	_, err = fileToUpload.Seek(0, 0)
	if err != nil {
		lm.logger.Fatal("Failed to seek to beginning of file to upload", zap.String("partitionName", lm.name))
	}
	lm.logger.Info("Uploading object", zap.Int64("fileSize", fileStats.Size()), zap.String("objectName", strconv.FormatUint(startOffset, 10)))
	lm.objectStorageClient.PutObject(context.TODO(), lm.name, strconv.FormatUint(startOffset, 10), fileToUpload, fileStats.Size(), minio.PutObjectOptions{})
	lm.archivedFiles = append(lm.archivedFiles, startOffset)
	// empty fileToUpload
	err = fileToUpload.Truncate(0)
	if err != nil {
		lm.logger.Fatal("Failed to empty log file written to object storage", zap.String("partitionName", lm.name), zap.Error(err))
	}
	_, err = fileToUpload.Seek(0, 0)
	if err != nil {
		lm.logger.Fatal("Failed to reset offset of file written to object storage", zap.String("partitionName", lm.name), zap.Error(err))
	}
	lm.prevNextFile = fileToUpload
}

func (lm *logMock) s3ObjectNames(startOffset uint64, endOffsetExclusively uint64) []string {
	objectNames := make([]string, 0)
	position, foundExactly := slices.BinarySearch(lm.archivedFiles, startOffset)
	if !foundExactly {
		position--
	}
	objectNames = append(objectNames, strconv.FormatUint(lm.archivedFiles[position], 10))
	position++
	for position < len(lm.archivedFiles) && lm.archivedFiles[position] < endOffsetExclusively {
		objectNames = append(objectNames, strconv.FormatUint(lm.archivedFiles[position], 10))
		position++
	}
	return objectNames
}

func (lm *logMock) Close() error {
	close(lm.quit)
	return nil
}
