package partitionmanager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lthiede/cartero/partition"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type PartitionManager struct {
	rwMutex             sync.RWMutex
	partitions          map[string]*partition.Partition
	objectStorageClient *minio.Client
	logger              *zap.Logger
	quit                chan struct{}
}

func minioClient(address string) (*minio.Client, error) {
	// Initialize minio client object.
	options := &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	}
	return minio.New(address, options)
}

func New(partitionNames []string, minioAddress string, logger *zap.Logger) (*PartitionManager, error) {
	minioClient, err := minioClient(minioAddress)
	if err != nil {
		return nil, fmt.Errorf("error trying to create minio client: %v", err)
	}
	logger.Info("Created object storage client")
	partitions := map[string]*partition.Partition{}
	for _, partitionName := range partitionNames {
		p, err := partition.New(partitionName, minioClient, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating partition %s: %v", partitionName, err)
		}
		partitions[partitionName] = p
	}
	pm := &PartitionManager{
		partitions:          partitions,
		objectStorageClient: minioClient,
		logger:              logger,
		quit:                make(chan struct{}),
	}
	return pm, nil
}

func (pm *PartitionManager) CreatePartition(partitionName string) error {
	pm.logger.Info("Received partition create request", zap.String("partitionName", partitionName))
	pm.rwMutex.Lock()
	defer pm.rwMutex.Unlock()
	if _, ok := pm.partitions[partitionName]; ok {
		pm.logger.Warn("Tried to create partition that already existed", zap.String("partitionName", partitionName))
		return nil
	}
	p, err := partition.New(partitionName, pm.objectStorageClient, pm.logger)
	if err != nil {
		return fmt.Errorf("error creating new partition: %v", err)
	}
	pm.partitions[partitionName] = p
	return nil
}

func (pm *PartitionManager) GetPartition(partitionName string) (*partition.Partition, error) {
	pm.logger.Info("Received partition get request", zap.String("partitionName", partitionName))
	pm.rwMutex.RLock()
	defer pm.rwMutex.RUnlock()
	p, ok := pm.partitions[partitionName]
	if !ok {
		return nil, errors.New("partition doesn't currently exist")
	} else {
		return p, nil
	}
}

func (pm *PartitionManager) Close() error {
	pm.logger.Info("Closing partition manager")
	close(pm.quit)
	for name, p := range pm.partitions {
		err := p.Close()
		if err != nil {
			pm.logger.Error("Error closing partition", zap.String("partitionName", name), zap.Error(err))
		}
	}
	return nil
}
