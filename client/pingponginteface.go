package client

import "time"

type PingPong interface {
	PingPong() error
	TimedPingPong() (time.Duration, error)
	Close() error
}
