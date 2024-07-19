package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lthiede/cartero/server"
	"github.com/pkg/profile"
	"go.uber.org/zap"
)

var cpuFlag = flag.Bool("c", false, "do cpu profiling")
var blockFlag = flag.Bool("b", false, "do block profiling")
var mutexFlag = flag.Bool("m", false, "do mutex profiling")
var objectStorageAddressFlag = flag.String("o", "172.18.94.80:9000", "address of minio")

func main() {
	flag.Parse()
	if *cpuFlag {
		defer profile.Start(profile.CPUProfile).Stop()
	} else if *blockFlag {
		defer profile.Start(profile.BlockProfile).Stop()
	} else if *mutexFlag {
		defer profile.Start(profile.MutexProfile).Stop()
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"./logs"}
	logger, err := config.Build()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()
	server, err := server.New([]string{}, "172.18.94.70:8080", *objectStorageAddressFlag, logger)
	if err != nil {
		logger.Panic("Error creating server", zap.Error(err))
	}
	defer server.Close()
	<-c
}
