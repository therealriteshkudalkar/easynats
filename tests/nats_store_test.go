package tests

import (
	"github.com/therealriteshkudalkar/easynats"
	"testing"
	"time"
)

func TestNATSStoreConnection(t *testing.T) {
	natsStore := easynats.NewNATSStore()
	natsStore.ConnectAndInitializeJetstream("nats://localhost:4222")
	time.Sleep(2 * time.Second)
	natsStore.CloseConnection()
	time.Sleep(2 * time.Second)
}
