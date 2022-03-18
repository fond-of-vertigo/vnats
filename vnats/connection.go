package vnats

import (
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"io"
)

type Connection interface {
	io.Closer

	// NewPublisher creates a publisher for the given streamName.
	// If the streamInfo does not exist, it is created.
	NewPublisher(streamName string) (Publisher, error)

	// NewSubscriber creates a subscriber for the given consumer name and subject.
	// Consumer will be created if it does not exist.
	NewSubscriber(consumerName string, subject string) (Subscriber, error)
}

type connection struct {
	nats bridge
	log  logger.Logger
}

// Connect connects to a NATS server/cluster
func Connect(servers []string, logger logger.Logger) (Connection, error) {
	conn := &connection{
		log: logger,
	}

	var err error
	conn.nats, err = makeNATSBridge(servers, logger)
	if err != nil {
		return nil, fmt.Errorf("NATS connection could not be created: %w", err)
	}

	return conn, nil
}

// NewPublisher creates a publisher for the given streamName.
// If the streamInfo does not exist, it is created.
func (c *connection) NewPublisher(streamName string) (Publisher, error) {
	return makePublisher(c, streamName, c.log)
}

// NewSubscriber creates a subscriber for the given consumer name and subject.
// Consumer will be created if it does not exist.
func (c *connection) NewSubscriber(consumerName string, subject string) (Subscriber, error) {
	return makeSubscriber(c, subject, consumerName, c.log)
}

// Close closes the nats connection and drains all messages.
func (c *connection) Close() error {
	c.log.Debugf("Closing NATS connection...")
	if err := c.nats.Drain(); err != nil {
		return fmt.Errorf("NATS connection could not be closed: %w", err)
	}
	c.log.Debugf("Closed NATS connection.")
	return nil
}
