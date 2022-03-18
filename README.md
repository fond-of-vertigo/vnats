# vnats library

This library acts as a facade in front of [NATS](https://github.com/nats-io/nats.go) and will be used for the internal
Middleware.

## Usage

For using vnats you will need to
1. Establish connection to NATS
2. Create Publisher/ Subscriber
3. Profit!

### Publisher

The publisher expects an `interface`, which makes it useful for publishing structs.

#### Example

```go
package main

import (
	"github.com/fond-of/vnats.go/vnats"
	"github.com/fond-of/logging.go/logger"
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

	// Create publisher bound to stream `PRODUCTS`
	publisher, err := conn.NewPublisher("PRODUCTS")
	if err != nil {
		log.Errorf("Could not create publisher: %v", err)
	}

	p := Product{
		Name:        "Example Product",
		Price:       "12,34",
		LastUpdated: time.Now(),
	}
	
	// Publish message to stream `PRODUCTS.PRICES` with a context bound, unique message ID 
	// msgID is used for deduplication
	msgID := fmt.Sprintf("%s-%s", p.Name, p.LastUpdated)
	if err := publisher.Publish("PRODUCTS.PRICES", p, msgID); err != nil {
		log.Errorf("Could not publish %v: %v", p, err)
	}
    
	// Close NATS connection
	if err := conn.Close(); err != nil {
		log.Errorf("NATS connection could not be closed: %v", err)
	}
}
```
---

### Subscriber

We use a pull-based subscriber by default, which scales horizontally.
The subscriber is asynchronous and pulls continuously for new messages.
A message handler is needed to process each message. 
The message will be passed as a slice of bytes `[]byte`.

#### Example

```go
package main

import (
	"github.com/fond-of/vnats.go/vnats"
	"github.com/fond-of/logging.go/logger"
	"time"
	"encoding/json"
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

	// Create Pull-Subscriber bound to consumer `EXAMPLE_CONSUMER` 
	// and the subject `PRODUCTS.PRICES`
	subscriber, err := conn.NewSubscriber("EXAMPLE_CONSUMER", "PRODUCTS.PRICES")
	if err != nil {
		log.Errorf("Could not create subscriber: %v", err)
	}
    
	// Subscribe and specify messageHandler
	subscriber.Subscribe(messageHandler)
    
	// Wait for stop signal (e.g. ctrl-C)
	waitForStopSignal()
	
	// Unsubscribe and close NATS connection
	if err := subscriber.Unsubscribe(); err != nil {
		log.Errorf("Subscriber could not be unsubscribed: %v", err)
	}
	
	if err := conn.Close(); err != nil {
		log.Errorf("NATS connection could not be closed: %v", err)
	}

}

func messageHandler(data []byte) error {
	var p Product
	
	if err := json.Unmarshal(data, &p); err != nil {
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
