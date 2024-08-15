package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/lthiede/cartero/client"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

var nFlag = flag.Uint64("n", 12_500, "end file offset exclusively")
var mFlag = flag.Uint64("m", 16_000_000, "file size in bytes")
var oFlag = flag.String("o", "localhost:9000", "address of object storage (s3/minio)")
var bFlag = flag.String("b", "messageservice", "name of the bucket to use")
var aFlag = flag.String("a", "minioadmin", "access key for s3")
var sFlag = flag.String("s", "minioadmin", "secret access key for s3")

func main() {
	flag.Parse()
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v \n", err)
		os.Exit(1)
	}
	logger.Info("Creating files", zap.Uint64("numberFiles", *nFlag), zap.Uint64("fileSize", *mFlag))
	objectStorageClient, err := client.MinioClient(*oFlag, *aFlag, *sFlag, 1)
	if err != nil {
		logger.Panic("Failed to create object storage client", zap.Error(err))
	}
	exists, err := objectStorageClient.BucketExists(context.Background(), *bFlag)
	if err != nil {
		logger.Panic("Failed to check if bucket exists", zap.Error(err), zap.String("bucketName", *bFlag))
	}
	if !exists {
		logger.Warn("Expected bucket to already exist. Creating bucket", zap.String("bucketName", *bFlag))
		err := objectStorageClient.MakeBucket(context.Background(), *bFlag, minio.MakeBucketOptions{})
		if err != nil {
			logger.Panic("Failed to create bucket", zap.Error(err), zap.String("bucketName", *bFlag))
		}
	}
	numberObjectsFound := 0
	for objectInfo := range objectStorageClient.ListObjects(context.Background(), *bFlag, minio.ListObjectsOptions{}) {
		if objectInfo.Err != nil {
			logger.Panic("Error looking for existing objects in bucket", zap.Error(objectInfo.Err), zap.String("bucketName", *bFlag))
		}
		logger.Error("Found object in bucket", zap.String("objectKey", objectInfo.Key))
		numberObjectsFound += 1
	}
	if numberObjectsFound > 0 {
		logger.Panic("Expected bucket to be empty", zap.Int("numberObjectsFound", numberObjectsFound))
	}
	for i := range *nFlag {
		_, err := objectStorageClient.PutObject(context.Background(), *bFlag, strconv.FormatUint(i, 10), rand.Reader, int64(*mFlag), minio.PutObjectOptions{})
		if err != nil {
			logger.Panic("Failed to upload object", zap.Error(err), zap.String("bucketName", *bFlag))
		}
	}
}
