package vnats

import (
	"github.com/nats-io/nats.go"
)

// A Header represents the key-value pairs.
type Header map[string][]string

// InMsg represents an incoming message.
type InMsg interface {
	// Subject represents the source subject name, like "PRODUCTS.new".
	Subject() string

	// Reply represents an optional subject name where a reply message should be sent to.
	// It is the responsibility of the Subscriber to send a response message to that subject.
	Reply() string

	// MsgID represents a unique value for the Data, like a hash value.
	// The same Data must lead to the same MsgID at any time.
	// E.g. two messages with the same Data must get the same MsgID.
	MsgID() string

	// Data represents the raw byte data.
	Data() []byte

	// Header represents the optional Header for a message.
	Header() Header
}

type inMsg struct {
	msg *nats.Msg
}

func makeInMsg(msg *nats.Msg) InMsg {
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
	return i.msg.Header.Get(nats.MsgIdHdr)
}

func (i *inMsg) Header() Header {
	return Header(i.msg.Header)
}
