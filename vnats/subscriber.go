package vnats

import (
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	// Subscribe expects a message handler which will be called whenever a new message is received.
	Subscribe(handler MsgHandler)
	// Unsubscribe unsubscribes to the related consumer.
	Unsubscribe() error
}

// MsgHandler returns the message as a slice of bytes and must be manually unmarshalled to the specific interface.
type MsgHandler func(data []byte) error

type subscriber struct {
	conn         *connection
	subscription subscription
	log          logger.Logger
	consumerName string
	quitSignal   chan bool
}

// Subscribe expects a message handler which will be called whenever a new message is received.
// The MsgHandler MUST finish its task in under 30 minutes.
func (s *subscriber) Subscribe(handler MsgHandler) {
	go func() {
		for {
			select {
			case <-s.quitSignal:
				s.log.Infof("Received signal to quit subscription go-routine.")
				return

			default:
				s.fetchMessages(handler)

			}
		}
	}()
}

func (s *subscriber) fetchMessages(handler MsgHandler) {
	msg, err := s.subscription.Fetch()
	if err != nil {
		if err == nats.ErrTimeout {
			s.log.Debugf("No new messages, timeout.")
		} else {
			s.log.Errorf("Failed to receive msg: %s", err.Error())
		}

		return
	}

	s.log.Debugf("Received Message - MsgID: %s, Data: %s", msg.Header.Get(nats.MsgIdHdr), string(msg.Data))

	if err = handler(msg.Data); err != nil {
		s.log.Errorf("Message handle error, will be NAKED: %v", err)

		if err := msg.Nak(); err != nil {
			s.log.Errorf("Nak failed: %v", err)
		}

		return
	}

	if err = msg.Ack(); err != nil {
		s.log.Errorf("Ack failed: %v", err)
	}
}

// Unsubscribe unsubscribes to the related consumer.
func (s *subscriber) Unsubscribe() error {
	if err := s.subscription.Unsubscribe(); err != nil {
		return err
	}
	s.log.Debugf("Unsubscribed to consumer %s", s.consumerName)
	return nil
}

func makeSubscriber(conn *connection, subject string, consumerName string, logger logger.Logger) (*subscriber, error) {
	sub, err := conn.nats.CreateSubscription(subject, consumerName)
	if err != nil {
		return nil, fmt.Errorf("subscriber could not be created: %w", err)
	}

	p := &subscriber{
		conn:         conn,
		subscription: sub,
		log:          logger,
		consumerName: consumerName,
		quitSignal:   make(chan bool),
	}
	return p, nil
}
