package vnats

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

// CreateSubscriber creates a new Subscriber that subscribes to a NATS stream.
func (c *Connection) CreateSubscriber(args CreateSubscriberArgs) (*Subscriber, error) {
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

// MsgHandler is the type of function the Subscriber has to implement to process an incoming message.
type MsgHandler func(msg Msg) error

// Subscriber subscribes to a NATS stream and handles incoming messages.
type Subscriber struct {
	conn         *Connection
	subscription *nats.Subscription
	log          LogFunc
	consumerName string
	handler      MsgHandler
	quitSignal   chan bool
}

// Subscribe subscribes to the NATS stream and starts a go-routine that handles incoming messages.
func (s *Subscriber) Subscribe(handler MsgHandler) (err error) {
	if s.handler != nil {
		return fmt.Errorf("handler is already set, don't call Subscribe() multiple times")
	}

	s.handler = handler

	go func() {
		for {
			select {
			case <-s.quitSignal:
				s.log(LogLevelInfo, "Received signal to quit subscription go-routine.")
				return
			default:
				s.processMessages()
			}
		}
	}()

	return nil
}

func (s *Subscriber) processMessages() {
	natsMsgs, err := s.subscription.Fetch(1) // Fetch only one msg at once to keep the order
	if errors.Is(err, nats.ErrTimeout) {     // ErrTimeout is expected/ no new messages, so we don't log it
		return
	} else if err != nil {
		s.log(LogLevelError, "Failed to receive msg: %v", err)
		return
	}
	for _, natsMsg := range natsMsgs {
		msg := makeMsg(natsMsg)

		if err = s.handler(msg); err != nil {
			s.log(LogLevelError, "Message handle error, will be NAKed: %v", err)
			if err := natsMsg.NakWithDelay(defaultNakDelay); err != nil {
				s.log(LogLevelError, "natsMsg.Nak() failed: %s", err)
			}
			continue
		}

		if err = natsMsg.Ack(); err != nil {
			s.log(LogLevelError, "natsMsg.Ack() failed: %v", err)
		}
	}
}

// Unsubscribe unsubscribes the consumer from the NATS stream.
func (s *Subscriber) Unsubscribe() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}

	s.handler = nil
	s.log(LogLevelInfo, "Unsubscribe consumer %s", s.consumerName)

	return nil
}
