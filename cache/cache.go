package cache

import (
	"fmt"
	"sync"

	pb "github.com/lthiede/cartero/proto"
	"go.uber.org/zap"
)

type Cache struct {
	partitionsMap map[string]*partitionCache
	logger        *zap.Logger
}

type partitionCache struct {
	messages          [][]byte
	lock              sync.Mutex
	startOffset       int
	offsetBroadcasted int
	consumers         []*Consumer
	Input             chan [][]byte
	quit              chan struct{}
}

type Consumer struct {
	Messages    chan *pb.ConsumeMessageBatch
	StartOffset int
	Quit        chan struct{}
}

func New(partitionNames []string, logger *zap.Logger) *Cache {
	cache := &Cache{
		partitionsMap: make(map[string]*partitionCache),
		logger:        logger,
	}
	for _, partitionName := range partitionNames {
		partitionCache := &partitionCache{
			Input: make(chan [][]byte),
			quit:  make(chan struct{}),
		}
		go partitionCache.handleInput()
		cache.partitionsMap[partitionName] = partitionCache
	}
	return cache
}

// TODO: Maybe make things batched
func (c *Cache) Write(messages [][]byte, partitionName string) error {
	partitionCache, ok := c.partitionsMap[partitionName]
	if !ok {
		return fmt.Errorf("unknown partition %s", partitionName)
	}
	partitionCache.Input <- messages
	return nil
}

func (partitionCache *partitionCache) handleInput() {
	for {
		select {
		case messages := <-partitionCache.Input:
			partitionCache.lock.Lock()
			partitionCache.messages = append(partitionCache.messages, messages...)
			i := 0 // output index
			startOffset := partitionCache.offsetBroadcasted
			endOffset := partitionCache.offsetBroadcasted + len(messages)
			for _, consumer := range partitionCache.consumers {
				select {
				case <-consumer.Quit:
				default:
					partitionCache.consumers[i] = consumer
					i++
					if consumer.StartOffset >= endOffset {
						continue
					}
					if consumer.StartOffset < startOffset {
						consumer.Messages <- &pb.ConsumeMessageBatch{
							StartOffset: uint64(startOffset),
							EndOffset:   uint64(endOffset),
							Messages:    &pb.Messages{Messages: messages},
						}
						continue
					}
					consumer.Messages <- &pb.ConsumeMessageBatch{
						StartOffset: uint64(consumer.StartOffset),
						EndOffset:   uint64(endOffset),
						Messages:    &pb.Messages{Messages: messages[consumer.StartOffset-startOffset:]},
					}
				}
			}
			partitionCache.consumers = partitionCache.consumers[:i]
			partitionCache.offsetBroadcasted = endOffset
			partitionCache.lock.Unlock()
		case <-partitionCache.quit:
			return
		}
	}
}

type CacheReadError struct {
	error
	ReadFromLog bool
}

func (c *Cache) StartReading(consumer *Consumer, partitionName string) *CacheReadError {
	partitionCache, ok := c.partitionsMap[partitionName]
	if !ok {
		return &CacheReadError{fmt.Errorf("unknown partition %s", partitionName), false}
	}
	partitionCache.lock.Lock()
	defer partitionCache.lock.Unlock()
	if consumer.StartOffset < partitionCache.startOffset {
		return &CacheReadError{fmt.Errorf("cache start offset %d past specified offset %d", partitionCache.startOffset, consumer.StartOffset), true}
	}
	if consumer.StartOffset < partitionCache.offsetBroadcasted {
		consumer.Messages <- &pb.ConsumeMessageBatch{
			StartOffset: uint64(consumer.StartOffset),
			EndOffset:   uint64(partitionCache.offsetBroadcasted),
			Messages:    &pb.Messages{Messages: partitionCache.messages[consumer.StartOffset:partitionCache.offsetBroadcasted]},
		}
	}
	partitionCache.consumers = append(partitionCache.consumers, consumer)
	return nil
}

func (c *Cache) Close() error {
	for _, partitionCache := range c.partitionsMap {
		close(partitionCache.quit)
	}
	return nil
}
