package vnats

import (
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/nats-io/nats.go"
	"strings"
	"time"
)

type Publisher interface {
	// Publish sends the message (data) to the given subject.
	Publish(args PublishArgs) error
}

type publisher struct {
	conn       *connection
	streamName string
	encoding   MsgEncoding
	log        logger.Logger
}

// PublishArgs contains the arguments publishing a new message.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type PublishArgs struct {
	// Subject is the destination subject name, like "PRODUCTS.new"
	Subject string

	// Reply contains the subject name where a reply should be sent to.
	// This value is optional.
	Reply string

	// MsgID must be unique value for the content, like a hash value.
	// So the same data must have the MsgID, no matter how often you send it.
	MsgID string

	// Data contains the data to send. Depends on the encoding how this value
	// is treated. If JSON encoding is enabled, this can be a struct which is the
	// marshalled to JSON automatically.
	// If raw encoding is used, this must be a byte array or a string.
	Data interface{}
}

func (p *publisher) Publish(args PublishArgs) error {
	if err := validateSubject(args.Subject, p.streamName); err != nil {
		return err
	}

	dataBytes, err := encodePayload(p.encoding, args.Data)
	if err != nil {
		return fmt.Errorf("encodePayload failed for msg %s @ %s: %w", args.MsgID, args.Subject, err)
	}

	p.log.Debugf("Publish message with msg-ID: %s @ %s\n", args.MsgID, args.Subject)
	msg := nats.Msg{
		Subject: args.Subject,
		Reply:   args.Reply,
		Data:    dataBytes,
	}

	if err = p.conn.nats.PublishMsg(&msg, args.MsgID); err != nil {
		return fmt.Errorf("message with msg-ID: %s @ %s could not be published: %w", args.MsgID, args.Subject, err)
	}
	return nil
}

func makePublisher(conn *connection, args *NewPublisherArgs) (*publisher, error) {
	if err := validateStreamName(args.StreamName); err != nil {
		return nil, err
	}
	_, err := conn.nats.GetOrAddStream(&nats.StreamConfig{
		Name:       args.StreamName,
		Subjects:   []string{args.StreamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(conn.nats.Servers()),
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	})

	if err != nil {
		return nil, fmt.Errorf("publisher could not be created: %w", err)
	}

	p := &publisher{
		conn:       conn,
		log:        conn.log,
		streamName: args.StreamName,
		encoding:   args.Encoding,
	}
	return p, nil
}

func validateSubject(subject string, streamName string) error {
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
