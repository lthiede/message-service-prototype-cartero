package client

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

type BenchmarkConsumer struct {
	objectStorageClient      *minio.Client
	bucketName               string
	objectNames              []string
	nextObjectBufferPosition int
	objectNamesLock          sync.Mutex
	objects                  []*benchmarkObjectInDownload
	downloadTasks            chan downloadTask
	CollectMetrics           bool
	CollectMetricsLock       sync.RWMutex
	returnLatencies          chan []time.Duration
	returnBytesDownloaded    chan uint64
	returnFilesDownloaded    chan int
	bytesConsumed            uint64
	filesConsumed            int
	logger                   *zap.Logger
	done                     chan struct{}
}

type downloadTask struct {
	name           string
	bufferPosition int
}

type benchmarkObjectInDownload struct {
	downloaded bool
	read       bool
	data       []byte
	lock       sync.Mutex
}

type MinioMetrics struct {
	FilesDownloaded    int
	BytesDownloaded    uint64
	FilesConsumed      int
	BytesConsumed      uint64
	FirstByteLatencies []time.Duration
}

func NewBenchmarkConsumer(bucketName string, endpoint, accessKey, secretAccessKey string, logger *zap.Logger) (*BenchmarkConsumer, error) {
	objectStorageClient, err := MinioClient(endpoint, accessKey, secretAccessKey, Concurrency)
	if err != nil {
		return nil, fmt.Errorf("failed to create object storage client: %v", err)
	}
	benchmarkConsumer := &BenchmarkConsumer{
		done:                  make(chan struct{}),
		objectStorageClient:   objectStorageClient,
		bucketName:            bucketName,
		downloadTasks:         make(chan downloadTask),
		objects:               make([]*benchmarkObjectInDownload, Concurrency),
		returnLatencies:       make(chan []time.Duration),
		returnBytesDownloaded: make(chan uint64),
		returnFilesDownloaded: make(chan int),
		logger:                logger,
	}
	for i := range benchmarkConsumer.objects {
		benchmarkConsumer.objects[i] = &benchmarkObjectInDownload{}
	}
	benchmarkConsumer.logger.Info("Finding downloadable objects for benchmark")
	objectNames := benchmarkConsumer.findDownloadableObjectsBenchmark()
	for i := range Concurrency {
		sliceLength := len(objectNames) / Concurrency
		objectNamesSlice := objectNames[i*sliceLength : (i+1)*sliceLength]
		go benchmarkConsumer.downloadObjectsBenchmark(objectNamesSlice)
	}
	return benchmarkConsumer, nil
}

func (c *BenchmarkConsumer) findDownloadableObjectsBenchmark() []string {
	objectNames := make([]string, 0)
	for objectInfo := range c.objectStorageClient.ListObjects(context.Background(), c.bucketName, minio.ListObjectsOptions{}) {
		if objectInfo.Err != nil {
			c.logger.Panic("Error looking for existing objects in bucket", zap.Error(objectInfo.Err), zap.String("bucketName", c.bucketName))
		}
		objectNames = append(objectNames, objectInfo.Key)
	}
	c.logger.Info("Learned about minio objects", zap.String("bucketName", c.bucketName), zap.Int("numberObjects", len(objectNames)))
	return objectNames
	// c.objectNamesLock.Lock()
	// c.objectNames = objectNames
	// c.objectNamesLock.Unlock()
	// bufferPosition := 0
	// for _, name := range objectNames {
	// 	c.downloadTasks <- downloadTask{
	// 		name:           name,
	// 		bufferPosition: bufferPosition,
	// 	}
	// 	bufferPosition = (bufferPosition + 1) % Concurrency
	// }
	// numObjects := len(objectNames)
	// index := 0
	// for {
	// 	select {
	// 	case <-c.done:
	// 		c.logger.Info("Stop feeding downloadable objects", zap.String("bucketName", c.bucketName))
	// 	default:
	// 		if index == 0 {
	// 			c.logger.Info("Starting new circle")
	// 		}
	// 		current := objectNames[index]
	// 		// objectInDownload := c.objects[bufferPosition]
	// 		// objectInDownload.lock.Lock()
	// 		// for !objectInDownload.read {
	// 		// 	objectInDownload.lock.Unlock()
	// 		// 	time.Sleep(10 * time.Millisecond)
	// 		// 	objectInDownload.lock.Lock()
	// 		// }
	// 		// objectInDownload.lock.Unlock()

	// 		// c.objectNamesLock.Lock()
	// 		// c.objectNames = append(c.objectNames, current)
	// 		// c.objectNamesLock.Unlock()
	// 		c.downloadTasks <- downloadTask{
	// 			name: current,
	// 			// bufferPosition: bufferPosition,
	// 		}
	// 		// bufferPosition = (bufferPosition + 1) % Concurrency
	// 		index = (index + 1) % numObjects
	// 	}
	// }
}

type firstByteRecorder struct {
	t *time.Time
	r io.Reader
}

func (f *firstByteRecorder) Read(p []byte) (n int, err error) {
	if f.t != nil || len(p) == 0 {
		return f.r.Read(p)
	}
	// Read a single byte.
	n, err = f.r.Read(p[:1])
	if n > 0 {
		t := time.Now()
		f.t = &t
	}
	return n, err
}

func (c *BenchmarkConsumer) downloadObjectsBenchmark(objectNames []string) {
	latencies := make([]time.Duration, 0)
	// allData := make([]byte, 0)
	filesDownloaded := 0
	var bytesDownloaded uint64
	index := 0
	for {
		select {
		case <-c.done:
			c.logger.Info("Stop downloading Objects", zap.String("bucketName", c.bucketName))
			c.returnLatencies <- latencies
			c.returnFilesDownloaded <- filesDownloaded
			c.returnBytesDownloaded <- bytesDownloaded
			c.logger.Info("Download objects routine returned metrics", zap.Int("numLatencies", len(latencies)), zap.Int("files", filesDownloaded), zap.Uint64("bytes", bytesDownloaded))
			return
		default:
			// benchmarkObjectInDownload := c.objects[downloadTask.bufferPosition]
			memStats := &runtime.MemStats{}
			runtime.ReadMemStats(memStats)
			// c.logger.Info("Starting download of new object", zap.String("objectName", downloadTask.name), zap.Int("bufferPosition", downloadTask.bufferPosition), zap.String("heapAlloc", message.NewPrinter(language.English).Sprintf("%d", memStats.HeapAlloc)))
			c.logger.Info("Starting download of new object", zap.String("objectName", objectNames[index]))
			// object, err := c.objectStorageClient.GetObject(context.TODO(), c.bucketName, downloadTask.name, minio.GetObjectOptions{})
			object, err := c.objectStorageClient.GetObject(context.TODO(), c.bucketName, objectNames[index], minio.GetObjectOptions{})
			if err != nil {
				// c.logger.Error("Failed to download object from s3", zap.Error(err), zap.String("objectName", downloadTask.name))
				c.logger.Error("Failed to download object from s3", zap.Error(err), zap.String("objectName", objectNames[index]))
				return
			}
			stats, err := object.Stat()
			if err != nil {
				// c.logger.Error("Failed to get object stats", zap.Error(err), zap.String("objectName", downloadTask.name))
				c.logger.Error("Failed to get object stats", zap.Error(err), zap.String("objectName", objectNames[index]))
				return
			}
			if stats.Size == 0 {
				// c.logger.Info("Downloaded object of size 0", zap.String("objectName", downloadTask.name))
				c.logger.Info("Downloaded object of size 0", zap.String("objectName", objectNames[index]))
				filesDownloaded++
				// benchmarkObjectInDownload.lock.Lock()
				// benchmarkObjectInDownload.downloaded = true
				// benchmarkObjectInDownload.lock.Unlock()
				continue
			}
			fbr := &firstByteRecorder{
				r: object,
			}
			// n, err := object.Read(benchmarkObjectInDownload.data)
			start := time.Now()
			n, err := io.Copy(io.Discard, fbr)
			if err != nil {
				c.logger.Error("Failed to copy object data", zap.Error(err))
			}
			if n != stats.Size {
				c.logger.Error("Read less bytes than expected", zap.Int64("expected", stats.Size), zap.Int64("read", n))
			}
			c.CollectMetricsLock.RLock()
			if c.CollectMetrics {
				c.logger.Info("Collecting metrics")
				latencies = append(latencies, fbr.t.Sub(start))
				filesDownloaded++
				bytesDownloaded += uint64(stats.Size)
			}
			c.CollectMetricsLock.RUnlock()
			// benchmarkObjectInDownload.lock.Lock()
			// benchmarkObjectInDownload.downloaded = true
			// benchmarkObjectInDownload.lock.Unlock()
		}
	}
}

func (c *BenchmarkConsumer) nextObjectName() (string, error) {
	timeSlept := 0 * time.Microsecond
	for {
		c.objectNamesLock.Lock()
		if len(c.objectNames) > 0 {
			nextObject := c.objectNames[0]
			c.objectNamesLock.Unlock()
			return nextObject, nil
		} else {
			c.objectNamesLock.Unlock()
			time.Sleep(10 * time.Microsecond)
			timeSlept += 10 * time.Microsecond
			if timeSlept >= NextObjectNameTimeout {
				c.logger.Error("Waiting for next object name timed out", zap.Float64("secondsWaited", timeSlept.Seconds()), zap.Float64("timeout", NextObjectNameTimeout.Seconds()))
				return "", fmt.Errorf("waited for object name for longer than %d ms", NextObjectNameTimeout.Milliseconds())
			}
		}
	}
}

func (c *BenchmarkConsumer) removeCurrentObject() {
	c.objectNamesLock.Lock()
	currentObjectName := c.objectNames[0]
	c.objectNames = c.objectNames[1:]
	c.objectNamesLock.Unlock()

	downloadedObject := c.objects[c.nextObjectBufferPosition]
	downloadedObject.read = true
	downloadedObject.downloaded = false
	c.logger.Info("Removed object from objects", zap.String("objectName", currentObjectName))
	c.nextObjectBufferPosition = (c.nextObjectBufferPosition + 1) % Concurrency
}

func (c *BenchmarkConsumer) NextObject() error {
	return nil
	objectName, err := c.nextObjectName()
	if err == ErrTimeout {
		return err
	}
	if err != nil {
		return fmt.Errorf("failed to get next object name: %v", err)
	}
	timeSlept := 0 * time.Microsecond
	benchmarkObject := c.objects[c.nextObjectBufferPosition]
	for {
		benchmarkObject.lock.Lock()
		if benchmarkObject.downloaded {
			break
		} else {
			benchmarkObject.lock.Unlock()
			if timeSlept >= Timeout {
				c.logger.Error("Waiting for next object timed out", zap.String("objectName", objectName))
				return ErrTimeout
			}
			time.Sleep(10 * time.Microsecond)
			timeSlept += 10 * time.Microsecond
		}
	}
	c.logger.Info("Read object", zap.String("objectName", objectName))
	benchmarkObject.read = true
	c.CollectMetricsLock.RLock()
	if c.CollectMetrics {
		c.bytesConsumed += uint64(len(benchmarkObject.data))
		c.filesConsumed++
	}
	c.CollectMetricsLock.RUnlock()
	benchmarkObject.lock.Unlock()
	c.removeCurrentObject()
	return nil
}

func (c *BenchmarkConsumer) Close() error {
	c.logger.Info("Finished consume call", zap.String("partitionName", c.bucketName))
	close(c.done)
	return nil
}

func (c *BenchmarkConsumer) Metrics() MinioMetrics {
	latencies := make([]time.Duration, 0)
	for range Concurrency {
		latencies = append(latencies, <-c.returnLatencies...)
	}
	filesDownloaded := 0
	var bytesDownloaded uint64
	for range Concurrency {
		filesDownloaded += <-c.returnFilesDownloaded
		bytesDownloaded += <-c.returnBytesDownloaded
	}
	return MinioMetrics{
		FirstByteLatencies: latencies,
		BytesDownloaded:    bytesDownloaded,
		FilesDownloaded:    filesDownloaded,
		BytesConsumed:      c.bytesConsumed,
		FilesConsumed:      c.filesConsumed,
	}
}
