package vnats

import (
	"github.com/nats-io/nats.go"
)

// A Header represents the key-value pairs.
type Header map[string][]string

// Msg contains the arguments publishing a new message.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type Msg struct {
	// Subject represents the destination subject name, like "PRODUCTS.new"
	Subject string

	// Reply represents an optional subject name where a reply message should be sent to.
	// This value is just distributed, whether the response is sent to the specified subject depends on the Subscriber.
	Reply string

	// MsgID represents a unique value for the message, like a hash value of Data.
	// Semantically equal messages must lead to the same MsgID at any time.
	// E.g. two messages with the same Data must have the same MsgID.
	//
	// The MsgID is used for deduplication.
	MsgID string

	// Data represents the raw byte data to send. The data is sent as-is.
	Data []byte

	// Header represents the optional Header for the message.
	Header Header
}

func NewMsg(subject, id string, data []byte) *Msg {
	return &Msg{
		Subject: subject,
		MsgID:   id,
		Data:    data,
	}
}
func makeMsg(msg *nats.Msg) Msg {
	return Msg{
		Subject: msg.Subject,
		Reply:   msg.Reply,
		MsgID:   msg.Header.Get(nats.MsgIdHdr),
		Data:    msg.Data,
		Header:  Header(msg.Header),
	}
}
func (m *Msg) toNATS() *nats.Msg {
	return &nats.Msg{
		Subject: m.Subject,
		Reply:   m.Reply,
		Data:    m.Data,
		Header:  nats.Header(m.Header),
	}
}
