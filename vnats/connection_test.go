package vnats

import (
	"github.com/fond-of/logging.go/logger"
	"testing"
)

var log = logger.New(logger.LvlDebug)

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	_, err := Connect([]string{"127.0.0.1:4222"}, log)
	if err != nil {
		t.Errorf("NATS connection could not be established: %v", err)
	}
}
