package vnats

import "github.com/nats-io/nats.go"

type subscription interface {
	Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
	Unsubscribe() error
	Drain() error
}
type natsSubscription struct {
	streamSubscription *nats.Subscription
}

func (s *natsSubscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return s.streamSubscription.Fetch(batch, opts...)
}

func (s *natsSubscription) Unsubscribe() error {
	return s.streamSubscription.Unsubscribe()
}

func (s *natsSubscription) Drain() error {
	return s.streamSubscription.Drain()

}
