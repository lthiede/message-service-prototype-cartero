package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	// "net/http"
	// _ "net/http/pprof"

	"github.com/lthiede/cartero/server"
	"go.uber.org/zap"
)

type stringSlice []string

// var cpuFlag = flag.Bool("c", false, "do cpu profiling")
// var blockFlag = flag.Bool("b", false, "do block profiling")
// var mutexFlag = flag.Bool("m", false, "do mutex profiling")
var sFlag = flag.String("s", "localhost:8080", "server address")
var logAddressFlag stringSlice

func (n *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*n))
}

func (n *stringSlice) Set(value string) error {
	*n = append(*n, value)
	return nil
}

func main() {
	flag.Var(&logAddressFlag, "o", "addresses of log nodes")
	flag.Parse()
	// go func() {
	// 	runtime.SetBlockProfileRate(1)
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"./logs"}
	logger, err := config.Build()
	if err != nil {
		log.Panicf("Error creating logger: %v", err)
	}
	defer logger.Sync()
	server, err := server.New([]string{"partition0"}, *sFlag, logAddressFlag, logger)
	if err != nil {
		logger.Panic("Error creating server", zap.Error(err))
	}
	defer server.Close()
	<-c
}
