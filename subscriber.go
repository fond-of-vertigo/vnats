package vnats

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

// NewSubscriber creates a new Subscriber that subscribes to a NATS consumer.
func (c *Connection) NewSubscriber(args NewSubscriberArgs) (*Subscriber, error) {
	subscription, err := c.nats.CreateSubscription(args.Subject, args.ConsumerName, args.Mode)
	if err != nil {
		return nil, fmt.Errorf("Subscriber could not be created: %w", err)
	}

	sub := &Subscriber{
		conn:         c,
		subscription: subscription,
		log:          c.log,
		consumerName: args.ConsumerName,
		quitSignal:   make(chan bool),
	}

	c.subscribers = append(c.subscribers, sub)
	return sub, nil
}

// MsgHandler is the type of function the Subscriber has to implement
// to process an incoming message.
type MsgHandler func(msg InMsg) error

// Subscriber subscribes to a NATS consumer and handles incoming messages.
type Subscriber struct {
	conn         *Connection
	subscription *nats.Subscription
	log          Log
	consumerName string
	handler      MsgHandler
	quitSignal   chan bool
}

// Subscribe subscribes to the NATS consumer and starts a go-routine that handles incoming messages.
func (s *Subscriber) Subscribe(handler MsgHandler) (err error) {
	if s.handler != nil {
		return fmt.Errorf("handler is already set, don't call Subscribe() multiple times")
	}

	s.handler = handler

	go func() {
		for {
			select {
			case <-s.quitSignal:
				s.log("Received signal to quit subscription go-routine.")
				return
			default:
				s.fetchMessages()
			}
		}
	}()

	return nil
}

func (s *Subscriber) fetchMessages() {
	msg, err := s.subscription.Fetch(1)
	if errors.Is(err, nats.ErrTimeout) { // ErrTimeout is expected/ no new messages, so we don't log it
		return
	} else if err != nil {
		s.log("Failed to receive msg: %v", err)
		return
	}
	firstMessage := msg[0]

	inMsg := makeInMsg(firstMessage)
	err = s.handler(inMsg)
	if err != nil {
		s.log("Message handle error, will be NAKed: %v", err)
		if err := firstMessage.NakWithDelay(defaultNakDelay); err != nil {
			s.log("msg.Nak() failed: %s", err)
		}
		return
	}

	if err = firstMessage.Ack(); err != nil {
		s.log("msg.Ack() failed: %v", err)
	}
}

// Unsubscribe unsubscribes from the NATS consumer.
func (s *Subscriber) Unsubscribe() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}

	s.handler = nil
	s.log("Unsubscribed to consumer %s", s.consumerName)

	return nil
}
