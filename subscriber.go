package vnats

import (
	"errors"
	"fmt"
	"github.com/fond-of/logging.go/logger"
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
	log          logger.Logger
	consumerName string
	handler      MsgHandler
	quitSignal   chan bool
}

func (s *subscriber) Subscribe(handler MsgHandler) (err error) {
	if s.handler != nil {
		return fmt.Errorf("handler is already set, don't call Subscribe() multiple times")
	}

	go func() {
		for {
			select {
			case <-s.quitSignal:
				s.log.Infof("Received signal to quit subscription go-routine.")
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
		if errors.Is(err, nats.ErrTimeout) {
			s.log.Debugf("No new messages, timeout")
		} else {
			s.log.Errorf("Failed to receive msg: %s", err.Error())
		}

		return
	}

	if s.log.IsDebugEnabled() {
		s.log.Debugf("Received Message - MsgID: %s, Data: %s", msg.Header.Get(nats.MsgIdHdr), string(msg.Data))
	}

	inMsg := makeInMsg(msg)
	err = s.handler(inMsg)
	if err != nil {
		s.log.Errorf("Message handle error, will be NAKed: %s", err)
		if err := msg.NakWithDelay(defaultNakDelay); err != nil {
			s.log.Errorf("msg.Nak() failed: %s", err)
		}
		return
	}

	if err = msg.Ack(); err != nil {
		s.log.Errorf("msg.Ack() failed: %s", err)
	}
}

func (s *subscriber) Unsubscribe() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}

	s.handler = nil
	s.log.Debugf("Unsubscribed to consumer %s", s.consumerName)

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
