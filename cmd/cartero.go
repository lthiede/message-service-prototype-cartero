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
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()
	server, err := server.New([]string{"partition0", "partition1", "partition2"}, logger)
	if err != nil {
		logger.Panic("Error creating server", zap.Error(err))
	}
	defer server.Close()
	<-c
}
