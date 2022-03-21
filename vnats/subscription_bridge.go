package vnats

import "github.com/nats-io/nats.go"

type subscription interface {
	Fetch() (Message, error)
	Unsubscribe() error
	Drain() error
}
type natsSubscription struct {
	streamSubscription *nats.Subscription
}

func (s *natsSubscription) Fetch() (Message, error) {
	messages, err := s.streamSubscription.Fetch(1)
	if err != nil {
		return nil, err
	}
	msg := messages[0]
	return &natsMsg{msg: msg}, nil
}

func (s *natsSubscription) Unsubscribe() error {
	return s.streamSubscription.Unsubscribe()
}

func (s *natsSubscription) Drain() error {
	return s.streamSubscription.Drain()

}
