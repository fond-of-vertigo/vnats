package vnats

import "github.com/nats-io/nats.go"

type Message interface {
	AckSync() error
	Data() []byte
	Subject() string
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

func (m *natsMsg) Subject() string {
	return m.msg.Subject
}
func NewMessage(subject string, data []byte) Message {
	msg := nats.Msg{Subject: subject, Data: data}
	return &natsMsg{msg: &msg}
}
