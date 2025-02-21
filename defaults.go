package vnats

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultStorageType       = nats.FileStorage
	defaultDuplicationWindow = time.Minute * 30
	defaultAckWait           = time.Second * 30
	defaultNakDelay          = time.Minute * 1
	defaultMaxAge            = time.Hour * 24 * 30
)
