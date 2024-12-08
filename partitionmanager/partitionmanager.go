package partitionmanager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lthiede/cartero/partition"
	"go.uber.org/zap"
)

type PartitionManager struct {
	rwMutex      sync.RWMutex
	partitions   map[string]*partition.Partition
	logAddresses []string
	logger       *zap.Logger
	quit         chan struct{}
}

const WriteQuorum = 3
const AckQuorum = 2

func New(partitionNames []string, logAddresses []string, logger *zap.Logger) (*PartitionManager, error) {
	partitions := map[string]*partition.Partition{}
	for _, partitionName := range partitionNames {
		p, err := partition.New(partitionName, logAddresses, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating partition %s: %v", partitionName, err)
		}
		partitions[partitionName] = p
	}
	pm := &PartitionManager{
		partitions:   partitions,
		logAddresses: logAddresses,
		logger:       logger,
		quit:         make(chan struct{}),
	}
	return pm, nil
}

func (pm *PartitionManager) CreatePartition(partitionName string, numPartitions uint32) error {
	pm.logger.Info("Received partition create request", zap.String("partitionName", partitionName))
	logAddressMapping, err := pm.shittyLogNodeLoadBalancing(numPartitions)
	if err != nil {
		return fmt.Errorf("error mapping partitions to log nodes: %v", err)
	}
	for i := range numPartitions {
		name := fmt.Sprintf("%s%d", partitionName, i)
		err := pm.createPartition(name, logAddressMapping[i])
		if err != nil {
			return fmt.Errorf("error creating partitions: %v", err)
		}
	}
	return nil
}

func (pm *PartitionManager) shittyLogNodeLoadBalancing(numPartitions uint32) ([][]string, error) {
	if numPartitions*WriteQuorum%uint32(len(pm.logAddresses)) != 0 {
		return nil,
			fmt.Errorf("creating a number of partitions that can't be evenly distributed on the log nodes is not supported. %d partitions, %d write quorum, %d log nodes",
				numPartitions,
				WriteQuorum,
				len(pm.logAddresses))
	}
	logAddressMapping := make([][]string, numPartitions)
	for i := range numPartitions {
		logAddressMapping[i] = pm.logAddresses[i*WriteQuorum : (i+1)*WriteQuorum]
	}
	return logAddressMapping, nil
}

func (pm *PartitionManager) createPartition(name string, logAddresses []string) error {
	pm.rwMutex.Lock()
	defer pm.rwMutex.Unlock()
	if _, ok := pm.partitions[name]; ok {
		pm.logger.Warn("Tried to create partition that already existed", zap.String("partitionName", name))
		return nil
	}
	p, err := partition.New(name, logAddresses, pm.logger)
	if err != nil {
		return fmt.Errorf("error creating partition %s: %v", name, err)
	}
	pm.partitions[name] = p
	return nil
}

func (pm *PartitionManager) DeletePartition(partitionName string, numPartitions uint32) error {
	pm.logger.Info("Received partition delete request", zap.String("partitionName", partitionName))
	for i := range numPartitions {
		name := fmt.Sprintf("%s%d", partitionName, i)
		err := pm.deletePartition(name)
		if err != nil {
			return fmt.Errorf("error deleting partitions: %v", err)
		}
	}
	return nil
}

func (pm *PartitionManager) deletePartition(name string) error {
	pm.rwMutex.Lock()
	defer pm.rwMutex.Unlock()
	p, ok := pm.partitions[name]
	if !ok {
		pm.logger.Warn("Tried to delete partition that (already) doesn't exist", zap.String("partitionName", name))
		return nil
	}
	delete(pm.partitions, name)
	err := p.Close()
	if err != nil {
		return fmt.Errorf("error closing partition %s: %v", name, err)
	}
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
