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
	NewPublisher(streamName string, encoding MsgEncoding) (Publisher, error)

	// NewSubscriber creates a subscriber for the given consumer name and subject.
	// Consumer will be created if it does not exist.
	NewSubscriber(consumerName string, subject string, encoding MsgEncoding, mode SubscriptionMode) (Subscriber, error)
}

// SubscriptionMode defines how the consumer and its subscriber are configured. This mode must be set accordingly
// to the use-case. If the order of messages should be strictly ordered, SingleSubscriberStrictMessageOrder should be used.
// If the message order is not important, but horizontal scaling is, use MultipleSubscribersAllowed.
type SubscriptionMode int

const (
	// MultipleSubscribersAllowed mode (default) enables multiple subscriber of one consumer for horizontal scaling.
	// The message order cannot be guaranteed when messages get NAKed/ MsgHandler for message returns error.
	MultipleSubscribersAllowed SubscriptionMode = iota

	// SingleSubscriberStrictMessageOrder mode enables strict order of messages. If messages get NAKed/ MsgHandler for
	// message returns error, the subscriber of consumer will retry the failed message until resolved. This blocks the
	// entire consumer, so that horizontal scaling is not effectively possible.
	SingleSubscriberStrictMessageOrder
)

type connection struct {
	nats        bridge
	log         logger.Logger
	subscribers []*subscriber
}

// Connect returns Connection to a NATS server/ cluster and enables Publisher and Subscriber creation.
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

func (c *connection) NewPublisher(streamName string, encoding MsgEncoding) (Publisher, error) {
	return makePublisher(c, streamName, encoding, c.log)
}

func (c *connection) NewSubscriber(consumerName string, subject string, encoding MsgEncoding, mode SubscriptionMode) (Subscriber, error) {
	sub, err := makeSubscriber(c, subject, consumerName, encoding, mode, c.log)
	if err != nil {
		return nil, err
	}
	c.subscribers = append(c.subscribers, sub)
	return sub, nil
}

// Close closes the NATS connection and drains all subscriptions.
func (c *connection) Close() error {
	c.log.Infof("Draining and closing open subscriptions..")
	for _, sub := range c.subscribers {
		if err := sub.subscription.Drain(); err != nil {
			return err
		}
		sub.quitSignal <- true
		close(sub.quitSignal)

	}
	c.log.Infof("Closed all open subscriptions.")

	c.log.Infof("Closing NATS connection...")
	if err := c.nats.Drain(); err != nil {
		return fmt.Errorf("NATS connection could not be closed: %w", err)
	}
	c.log.Infof("Closed NATS connection.")
	return nil
}
