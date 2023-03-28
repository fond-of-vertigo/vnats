package vnats

import (
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	// Subscribe expects a message handler which will be called whenever a new message is received.
	// The MsgHandler MUST finish its task in under 30 seconds.
	Subscribe(handler MsgHandler) error

	// Unsubscribe unsubscribes to the related consumer.
	Unsubscribe() error
}

// MsgHandler is the type of function the subscriber has to implement
// to process an incoming message.
type MsgHandler func(msg InMsg) error

type subscriber struct {
	conn         *connection
	subscription subscription
	log          Log
	consumerName string
	handler      MsgHandler
	quitSignal   chan bool
}

func (s *subscriber) Subscribe(handler MsgHandler) (err error) {
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

func (s *subscriber) fetchMessages() {
	msg, err := s.subscription.Fetch()
	if err != nil {
		if !errors.Is(err, nats.ErrTimeout) { // ErrTimeout is expected/ no new messages, so we don't log it
			s.log("Failed to receive msg: %s", err)
		}

		return
	}

	inMsg := makeInMsg(msg)
	err = s.handler(inMsg)
	if err != nil {
		s.log("Message handle error, will be NAKed: %s", err)
		if err := msg.NakWithDelay(defaultNakDelay); err != nil {
			s.log("msg.Nak() failed: %s", err)
		}
		return
	}

	if err = msg.Ack(); err != nil {
		s.log("msg.Ack() failed: %s", err)
	}
}

func (s *subscriber) Unsubscribe() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}

	s.handler = nil
	s.log("Unsubscribed to consumer %s", s.consumerName)

	return nil
}

func makeSubscriber(conn *connection, args *NewSubscriberArgs) (*subscriber, error) {
	sub, err := conn.nats.CreateSubscription(args.Subject, args.ConsumerName, args.Mode)
	if err != nil {
		return nil, fmt.Errorf("subscriber could not be created: %w", err)
	}

	p := &subscriber{
		conn:         conn,
		subscription: sub,
		log:          conn.log,
		consumerName: args.ConsumerName,
		quitSignal:   make(chan bool),
	}

	return p, nil
}
