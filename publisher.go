package vnats

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Publisher is a NATS publisher that publishes to a NATS stream.
type Publisher struct {
	conn       *Connection
	streamName string
	log        Log
}

// NewPublisher creates a new Publisher that publishes to a NATS stream.
func (c *Connection) NewPublisher(args NewPublisherArgs) (*Publisher, error) {
	if err := validateStreamName(args.StreamName); err != nil {
		return nil, err
	}
	_, err := c.nats.GetOrAddStream(&nats.StreamConfig{
		Name:       args.StreamName,
		Subjects:   []string{args.StreamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(c.nats.Servers()),
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	})
	if err != nil {
		return nil, fmt.Errorf("Publisher could not be created: %w", err)
	}

	p := &Publisher{
		conn:       c,
		log:        c.log,
		streamName: args.StreamName,
	}
	return p, nil
}

// OutMsg contains the arguments publishing a new message.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type OutMsg struct {
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

// Publish sends the message (data) to the given subject.
func (p *Publisher) Publish(outMsg *OutMsg) error {
	if err := validateSubject(outMsg.Subject, p.streamName); err != nil {
		return err
	}

	msg := nats.Msg{
		Subject: outMsg.Subject,
		Reply:   outMsg.Reply,
		Data:    outMsg.Data,
		Header:  nats.Header(outMsg.Header),
	}

	err := p.conn.nats.PublishMsg(&msg, outMsg.MsgID)
	if err != nil {
		return fmt.Errorf("message with msgID: %s @ %s could not be published: %w", outMsg.MsgID, outMsg.Subject, err)
	}
	return nil
}

func validateSubject(subject, streamName string) error {
	if err := validateStreamName(streamName); err != nil {
		return err
	}
	if subject == "" {
		return fmt.Errorf("subject cannot be empty")
	}
	if !strings.HasPrefix(subject, streamName+".") {
		return fmt.Errorf("subject needs to begin with `STREAM_NAME.`")
	}
	return nil
}

func validateStreamName(streamName string) error {
	if streamName == "" {
		return fmt.Errorf("streamName cannot be empty")
	}
	if strings.ContainsAny(streamName, "*.>") {
		return fmt.Errorf("streamName cannot contain any of chars: *.>")
	}
	return nil
}
