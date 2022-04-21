# vnats library

This library acts as a facade in front of [NATS](https://github.com/nats-io/nats.go) and will be used for the internal
Middleware.

## Usage

For using vnats you will need to

1. Establish connection to NATS
2. Create Publisher/ Subscriber
3. Profit!

### Publisher

The publisher sends a slice of bytes `[]byte` to a subject. If a struct or different type should be sent, the user has
to (un-)marshal the payload.

#### Example

```go
package main

import (
	"encoding/json"
	"github.com/fond-of-vertigo/vnats"
	"github.com/fond-of-vertigo/logger"
	"fmt"
	"time"
)

type Product struct {
	Name        string
	Price       string
	LastUpdated time.Time
}

var log = logger.New(logger.LvlDebug)

// Define NATS server/ cluster
var server = []string{"nats://ruser:T0pS3cr3t@localhost:4222"}

func main() {
	// Establish connection to NATS server
	conn, err := vnats.Connect(server, log)
	if err != nil {
		log.Errorf(err.Error())
	}
	// Close NATS connection deferred
	defer func(conn vnats.Connection) {
		if err := conn.Close(); err != nil {
			log.Errorf("NATS connection could not be closed: %v", err)
		}
	}(conn)

	// Create publisher bound to stream `PRODUCTS`
	pub, err := conn.NewPublisher(vnats.NewPublisherArgs{StreamName: "PRODUCTS"})
	if err != nil {
		log.Errorf("Could not create publisher: %v", err)
	}

	p := Product{
		Name:        "Example Product",
		Price:       "12,34",
		LastUpdated: time.Now(),
	}

	// Since vnats needs a slice of bytes, the products is converted via the json marshaller
	productToBytes, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	// Publish message to stream `PRODUCTS.PRICES` with a context bound, unique message ID 
	// msgID is used for deduplication
	msgID := fmt.Sprintf("%s-%s", p.Name, p.LastUpdated)
	if err := pub.Publish(&vnats.OutMsg{
		Subject: "PRODUCTS.PRICES",
		MsgID:   msgID,
		Data:    productToBytes,
	}); err != nil {
		log.Errorf("Could not publish %v: %v", p, err)
	}
}
```

---

### Subscriber

We use a pull-based subscriber by default, which scales horizontally. The subscriber is asynchronous and pulls
continuously for new messages. A message handler is needed to process each message. The message will be passed as a
slice of bytes `[]byte`.

**Important**: The `MsgHandler` **MUST** finish its task under 30 seconds. Longer tasks must be only triggered and
executed asynchronously.

#### Example

```go
package main

import (
	"encoding/json"
	"github.com/fond-of-vertigo/vnats"
	"github.com/fond-of-vertigo/logger"
	"time"
	"os"
	"os/signal"
)

type Product struct {
	Name        string
	Price       string
	LastUpdated time.Time
}

var log = logger.New(logger.LvlDebug)

// Define NATS server/ cluster
var server = []string{"nats://ruser:T0pS3cr3t@localhost:4222"}

func main() {
	// Establish connection to NATS server
	conn, err := vnats.Connect(server, log)
	if err != nil {
		log.Errorf(err.Error())
	}
	
	// Unsubscribe to all open subscriptions and close NATS connection deferred
	defer func(conn vnats.Connection) {
		if err := conn.Close(); err != nil {
			log.Errorf("NATS connection could not be closed: %v", err)
		}
	}(conn)

	// Create Pull-Subscriber bound to consumer `EXAMPLE_CONSUMER` 
	// and the subject `PRODUCTS.PRICES`
	sub, err := conn.NewSubscriber(vnats.NewSubscriberArgs{
		ConsumerName: "EXAMPLE_CONSUMER",
		Subject:      "PRODUCTS.PRICES",
	})
	if err != nil {
		log.Errorf("Could not create subscriber: %v", err)
	}

	// Subscribe and specify messageHandler
	if err := sub.Subscribe(msgHandler); err != nil {
		log.Errorf(err.Error())
	}

	// Wait for stop signal (e.g. ctrl-C)
	waitForStopSignal()
}

// MsgHandler returns the data in a slice of bytes inside the InMsg struct.
func msgHandler(msg vnats.InMsg) error {
	var p Product
	if err := json.Unmarshal(msg.Data(), &p); err != nil {
		return err
	}
	log.Debugf("Received product: %v", p)
	return nil
}

func waitForStopSignal() {
	// Setting up signal capturing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Waiting for SIGINT (pkill -2)
	<-stop
}

```
