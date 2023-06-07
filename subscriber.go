package vnats

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

// NewSubscriber creates a new Subscriber that subscribes to a NATS stream.
func (c *Connection) NewSubscriber(args SubscriberArgs) (*Subscriber, error) {
	subscription, err := c.nats.Subscribe(args.Subject, args.ConsumerName, args.Mode)
	if err != nil {
		return nil, fmt.Errorf("subscriber could not be created: %w", err)
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

// MsgHandler is the type of function the Subscriber has to implement to process an incoming message.
type MsgHandler func(msg Msg) error

// Subscriber subscribes to a NATS consumer and pulls messages to handle by MsgHandler.
type Subscriber struct {
	conn         *Connection
	subscription *nats.Subscription
	log          LogFunc
	consumerName string
	handler      MsgHandler
	quitSignal   chan bool
}

// Start subscribes to the NATS consumer and starts a go-routine that handles pulled messages.
func (s *Subscriber) Start(handler MsgHandler) (err error) {
	if s.handler != nil {
		return fmt.Errorf("handler is already set, don't call Start() multiple times")
	}

	s.handler = handler

	go func() {
		for {
			select {
			case <-s.quitSignal:
				s.log("Received signal to quit subscription go-routine.")
				return
			default:
				s.processMessages()
			}
		}
	}()

	return nil
}

// Stop unsubscribes the consumer from the NATS stream.
func (s *Subscriber) Stop() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}

	s.handler = nil
	s.log("Unsubscribed consumer %s", s.consumerName)

	return nil
}

func (s *Subscriber) processMessages() {
	natsMsgs, err := s.subscription.Fetch(1) // Fetch only one msg at once to keep the order
	if errors.Is(err, nats.ErrTimeout) {     // ErrTimeout is expected/ no new messages, so we don't log it
		return
	} else if err != nil {
		s.log("Failed to receive msg: %v", err)
		return
	}

	msg := makeMsg(natsMsgs[0])
	if err = s.handler(msg); err != nil {
		s.log("Message handle error, will be NAKed: %v", err)
		if err := natsMsgs[0].NakWithDelay(defaultNakDelay); err != nil {
			s.log("natsMsg.Nak() failed: %s", err)
		}
		return
	}

	if err = natsMsgs[0].Ack(); err != nil {
		s.log("natsMsg.Ack() failed: %v", err)
	}
}
