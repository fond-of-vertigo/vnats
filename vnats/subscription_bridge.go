package vnats

import "github.com/nats-io/nats.go"

type message interface {
	AckSync() error
	Data() []byte
	MsgID() string
}

type natsMsg struct {
	msg *nats.Msg
}

func (m *natsMsg) AckSync() error {
	return m.msg.AckSync()
}

func (m *natsMsg) Data() []byte {
	return m.msg.Data
}
func (m *natsMsg) MsgID() string {
	return m.msg.Header.Get("Nats-Msg-Id")
}

type subscription interface {
	Fetch() (message, error)
	Unsubscribe() error
	Drain() error
}
type natsSubscription struct {
	streamSubscription *nats.Subscription
}

func (s *natsSubscription) Fetch() (message, error) {
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
