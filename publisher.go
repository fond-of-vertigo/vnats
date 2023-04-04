package vnats

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// CreatePublisher creates a new Publisher that publishes to a NATS stream.
func (c *Connection) CreatePublisher(args CreatePublisherArgs) (*Publisher, error) {
	if err := validateStreamName(args.StreamName); err != nil {
		return nil, err
	}
	_, err := c.nats.FetchOrAddStream(&nats.StreamConfig{
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

// Publisher is a NATS publisher that publishes to a NATS stream.
type Publisher struct {
	conn       *Connection
	streamName string
	log        LogFunc
}

// Publish publishes the message (data) to the given subject.
func (p *Publisher) Publish(msg *Msg) error {
	if err := validateSubject(msg.Subject, p.streamName); err != nil {
		return err
	}

	natsMsg := msg.toNATS()

	err := p.conn.nats.PublishMsg(natsMsg, msg.MsgID)
	if err != nil {
		return fmt.Errorf("message with msgID: %s @ %s could not be published: %w", msg.MsgID, msg.Subject, err)
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
