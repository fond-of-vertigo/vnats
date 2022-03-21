package vnats

import "github.com/nats-io/nats.go"

type Message interface {
	// AckSync acknowledges successful Message processing.
	AckSync() error
	// InProgress tells Server that the Message processing is still ongoing.
	InProgress() error
	// Data Returns payload Data of this Message.
	Data() []byte
	// Subject under which this Message is sent.
	Subject() string
	// MsgID for de-duplication relative to the duplication-time-window of each streamInfo.
	MsgID() string
}

type natsMsg struct {
	msg *nats.Msg
}

func (m *natsMsg) AckSync() error {
	return m.msg.AckSync()
}

func (m *natsMsg) InProgress() error {
	return m.msg.InProgress()
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
