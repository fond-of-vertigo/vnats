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
	// Optionally you can set a reply subject, if not required use the NoReply const.
	// Each message has a msgID for de-duplication relative to
	// the duplication-time-window of each stream.
	// The msgID must be unique for the content, like a hash value.
	// So the same data must have the msgID, no matter how often you send it.
	Publish(subject, reply, msgID string, data interface{}) error
}

const NoReply = ""

type publisher struct {
	conn       *connection
	streamName string
	encoding   MsgEncoding
	log        logger.Logger
}

func (p *publisher) Publish(subject, reply, msgID string, data interface{}) error {
	if err := validateSubject(subject, p.streamName); err != nil {
		return err
	}

	dataBytes, err := encodePayload(p.encoding, data)
	if err != nil {
		return fmt.Errorf("encodePayload failed for msg %s @ %s: %w", msgID, subject, err)
	}

	p.log.Debugf("Publish message with msg-ID: %s @ %s\n", msgID, subject)
	msg := nats.Msg{
		Subject: subject,
		Reply:   reply,
		Data:    dataBytes,
	}

	if err = p.conn.nats.PublishMsg(&msg, msgID); err != nil {
		return fmt.Errorf("message with msg-ID: %s @ %s could not be published: %w", msgID, subject, err)
	}
	return nil
}

func makePublisher(conn *connection, streamName string, encoding MsgEncoding, logger logger.Logger) (*publisher, error) {
	if err := validateStreamName(streamName); err != nil {
		return nil, err
	}
	_, err := conn.nats.GetOrAddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName + ".>"},
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
		log:        logger,
		streamName: streamName,
		encoding:   encoding,
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
