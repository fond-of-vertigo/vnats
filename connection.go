package vnats

import (
	"fmt"
	"io"
)

type Connection interface {
	io.Closer

	// NewPublisher creates a publisher for the given streamName.
	// If the streamInfo does not exist, it is created.
	NewPublisher(args NewPublisherArgs) (Publisher, error)

	// NewSubscriber creates a subscriber for the given consumer name and subject.
	// Consumer will be created if it does not exist.
	NewSubscriber(args NewSubscriberArgs) (Subscriber, error)
}

// SubscriptionMode defines how the consumer and its subscriber are configured. This mode must be set accordingly
// to the use-case. If the order of messages should be strictly ordered, SingleSubscriberStrictMessageOrder should be
// used. If the message order is not important, but horizontal scaling is, use MultipleSubscribersAllowed.
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

type Log func(format string, a ...interface{})

type connection struct {
	nats        bridge
	log         Log
	subscribers []*subscriber
}
type Option func(*connection)

// Connect returns Connection to a NATS server/ cluster and enables Publisher and Subscriber creation.
func Connect(servers []string, options ...Option) (Connection, error) {
	conn := &connection{
		log: func(_ string, _ ...interface{}) {},
	}

	conn.applyOptions(options...)

	bridge, err := makeNATSBridge(servers, conn.log)
	if err != nil {
		return nil, fmt.Errorf("NATS connection could not be created: %w", err)
	}

	conn.nats = bridge
	return conn, nil
}

func (c *connection) applyOptions(options ...Option) {
	for _, option := range options {
		option(c)
	}
}

// NewPublisherArgs contains the arguments for creating a new publisher.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type NewPublisherArgs struct {
	// StreamName is the name of the stream like "PRODUCTS" or "ORDERS".
	// If it does not exist, the stream will be created.
	StreamName string
}

func (c *connection) NewPublisher(args NewPublisherArgs) (Publisher, error) {
	return makePublisher(c, &args)
}

// NewSubscriberArgs contains the arguments for creating a new subscriber.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type NewSubscriberArgs struct {
	// ConsumerName contains the name of the consumer. By default, this should be the
	// name of the service.
	ConsumerName string

	// Subject defines which subjects of the stream should be subscribed.
	// Examples:
	//  "ORDERS.new" -> subscribe subject "new" of stream "ORDERS"
	//  "ORDERS.>"   -> subscribe all subjects in any level of stream "ORDERS".
	//  "ORDERS.*"   -> subscribe all direct subjects of stream "ORDERS", like "ORDERS.new", "ORDERS.processed",
	//                  but not "ORDERS.new.error".
	Subject string

	// Mode defines the constraints of the subscription. Default is MultipleSubscribersAllowed.
	// See SubscriptionMode for details.
	Mode SubscriptionMode
}

func (c *connection) NewSubscriber(args NewSubscriberArgs) (Subscriber, error) {
	sub, err := makeSubscriber(c, &args)
	if err != nil {
		return nil, err
	}
	c.subscribers = append(c.subscribers, sub)
	return sub, nil
}

// Close closes the NATS connection and drains all subscriptions.
func (c *connection) Close() error {
	c.log("Draining and closing open subscriptions..")
	for _, sub := range c.subscribers {
		if err := sub.subscription.Drain(); err != nil {
			return err
		}
		sub.quitSignal <- true
		close(sub.quitSignal)

	}
	c.log("Closed all open subscriptions.")
	c.log("Closing NATS connection...")
	if err := c.nats.Drain(); err != nil {
		return fmt.Errorf("NATS connection could not be closed: %w", err)
	}
	c.log("Closed NATS connection.")
	return nil
}

func WithLogger(log Log) Option {
	return func(c *connection) {
		c.log = log
	}
}
