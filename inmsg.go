package vnats

import "github.com/nats-io/nats.go"

type InMsg interface {
	Subject() string
	Reply() string
	Data() []byte
	MsgID() string
	Header() nats.Header
}

type inMsg struct {
	msg *nats.Msg
}

func makeInMsg(msg *nats.Msg) *inMsg {
	return &inMsg{msg: msg}
}

func (i *inMsg) Subject() string {
	return i.msg.Subject
}

func (i *inMsg) Reply() string {
	return i.msg.Reply
}

func (i *inMsg) Data() []byte {
	return i.msg.Data
}

func (i *inMsg) MsgID() string {
	return i.msg.Header.Get("Nats-Msg-Id")
}

func (i *inMsg) Header() nats.Header {
	return i.msg.Header
}
