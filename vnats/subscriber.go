package vnats

import (
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	Subscribe(handler MsgHandler)
	Unsubscribe() error
}

// MsgHandler returns the message as a byte array and must be manually unmarshalled to the specific interface.

type MsgHandler func(data []byte) error

type subscriber struct {
	conn         *connection
	subscription *nats.Subscription
	log          logger.Logger
	consumerName string
}

func (s *subscriber) Subscribe(handler MsgHandler) {
	go func() {
		for {
			messages, err := s.subscription.Fetch(1)
			if err != nil {
				if err == nats.ErrTimeout {
					s.log.Debugf("No new messages, timeout.")
				} else {
					s.log.Errorf("Failed to receive msg: %s", err.Error())
				}
				continue
			}

			msg := messages[0]
			s.log.Debugf("Receive msg: %s: %s", msg.Reply, string(msg.Data))

			if err = handler(msg.Data); err != nil {
				s.log.Errorf("Message handle error: %v", err)

			}
			if err = msg.AckSync(); err != nil {
				s.log.Errorf("AckSync failed: %v", err)
			}
		}
	}()
}

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
	}
	return p, nil
}
