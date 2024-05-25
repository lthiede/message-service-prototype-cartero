package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"./logs"}
	logger, err := config.Build()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()
	server, err := server.New([]string{"partition0", "partition1", "partition2"}, "172.18.94.80:8080", logger)
	if err != nil {
		logger.Panic("Error creating server", zap.Error(err))
	}
	defer server.Close()
	<-c
}
