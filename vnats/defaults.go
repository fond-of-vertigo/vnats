package vnats

import (
	"github.com/nats-io/nats.go"
	"time"
)

const defaultStorageType = nats.FileStorage
const defaultDuplicationWindow = time.Minute * 30
