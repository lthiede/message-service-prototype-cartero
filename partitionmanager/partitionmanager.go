package partitionmanager

import (
	"fmt"

	"github.com/lthiede/cartero/partition"
	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

type PartitionManager struct {
	GetPartitionRequests    chan GetPartitionRequest
	CreatePartitionRequests chan CreatePartitionRequest
	partitions              map[string]*partition.Partition
	logger                  *zap.Logger
	quit                    chan struct{}
}

type GetPartitionRequest struct {
	PartitionName string
	Partition     chan *partition.Partition
}

type CreatePartitionRequest struct {
	PartitionName string
	SendResponse  func(*pb.CreatePartitionResponse)
}

func New(partitionNames []string, logger *zap.Logger) (*PartitionManager, error) {
	partitions := map[string]*partition.Partition{}
	for _, partitionName := range partitionNames {
		p, err := partition.New(partitionName, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating partition %s: %v", partitionName, err)
		}
		partitions[partitionName] = p
	}
	pm := &PartitionManager{
		CreatePartitionRequests: make(chan CreatePartitionRequest),
		GetPartitionRequests:    make(chan GetPartitionRequest),
		partitions:              partitions,
		logger:                  logger,
		quit:                    make(chan struct{}),
	}
	go pm.handlePartitionUpdates()
	return pm, nil
}

func (pm *PartitionManager) handlePartitionUpdates() {
	for {
		select {
		case <-pm.quit:
			pm.logger.Info("Stop accepting update partition updates")
			return
		case createReq := <-pm.CreatePartitionRequests:
			pm.logger.Info("Received partition create request", zap.String("partitionName", createReq.PartitionName))
			_, ok := pm.partitions[createReq.PartitionName]
			if ok {
				pm.logger.Warn("Tried to create partition that already existed", zap.String("partitionName", createReq.PartitionName))
				createReq.SendResponse(&pb.CreatePartitionResponse{
					Successful:    true,
					PartitionName: createReq.PartitionName,
				})
				continue
			}
			p, err := partition.New(createReq.PartitionName, pm.logger)
			if err != nil {
				pm.logger.Error("Error creating new partition", zap.Error(err))
				createReq.SendResponse(&pb.CreatePartitionResponse{
					Successful:    false,
					PartitionName: createReq.PartitionName,
				})
				continue
			}
			pm.partitions[createReq.PartitionName] = p
			createReq.SendResponse(&pb.CreatePartitionResponse{
				Successful:    true,
				PartitionName: createReq.PartitionName,
			})
		case getReq := <-pm.GetPartitionRequests:
			pm.logger.Info("Received partition get request", zap.String("partitionName", getReq.PartitionName))
			p, ok := pm.partitions[getReq.PartitionName]
			if !ok {
				close(getReq.Partition)
			} else {
				getReq.Partition <- p
			}
		}
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
