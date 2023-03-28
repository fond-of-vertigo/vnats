package vnats

import (
	"github.com/nats-io/nats.go"
	"time"
)

const (
	defaultStorageType       = nats.FileStorage
	defaultDuplicationWindow = time.Minute * 30
	defaultAckWait           = time.Second * 30
	defaultNakDelay          = time.Second * 3
)
