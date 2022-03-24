package vnats

import "github.com/nats-io/nats.go"

type subscription interface {
	Fetch() (*nats.Msg, error)
	Unsubscribe() error
	Drain() error
}
type natsSubscription struct {
	streamSubscription *nats.Subscription
}

func (s *natsSubscription) Fetch() (*nats.Msg, error) {
	messages, err := s.streamSubscription.Fetch(1)
	if err != nil {
		return nil, err
	}
	msg := messages[0]
	return msg, nil
}

func (s *natsSubscription) Unsubscribe() error {
	return s.streamSubscription.Unsubscribe()
}

func (s *natsSubscription) Drain() error {
	return s.streamSubscription.Drain()

}
