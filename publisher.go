package vnats

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// StreamOpt is a functional option for configuring the stream.
type StreamOpt func(c *nats.StreamConfig)

// WithMaxAge sets the maximum retention time for the stream.
func WithMaxAge(maxAge time.Duration) StreamOpt {
	return func(c *nats.StreamConfig) {
		c.MaxAge = maxAge
	}
}

// WithReplicas sets the number of replicas for the stream.
func WithReplicas(replicas int) StreamOpt {
	return func(c *nats.StreamConfig) {
		c.Replicas = validateReplicas(replicas, c.Replicas)
	}
}

// WithSubjects adds additional subjects to the stream
func WithSubjects(subjects ...string) StreamOpt {
	return func(c *nats.StreamConfig) {
		for _, subject := range subjects {
			if !slices.Contains(c.Subjects, subject) {
				c.Subjects = append(c.Subjects, subject)
			}
		}
	}
}

// MustMakePublisher creates a new Publisher that publishes to a NATS stream.
// streamName is the name of the stream to publish to e.g. ORDERS, AVAILABILITIES
// if it does not exist, it will be created.
func (c *Connection) MustMakePublisher(streamName string, opts ...StreamOpt) *Publisher {
	pub, err := c.NewPublisher(streamName, opts...)
	if err != nil {
		panic(err)
	}
	return pub
}

// NewPublisher creates a new Publisher that publishes to a NATS stream.
// streamName is the name of the stream to publish to e.g. ORDERS, AVAILABILITIES
// if it does not exist, it will be created.
func (c *Connection) NewPublisher(streamName string, opts ...StreamOpt) (*Publisher, error) {
	if err := validateStreamName(streamName); err != nil {
		return nil, err
	}

	streamConfig := &nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(c.nats.Servers()),
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	}

	for _, opt := range opts {
		opt(streamConfig)
	}

	if err := c.nats.EnsureStreamExists(streamConfig); err != nil {
		return nil, fmt.Errorf("publisher could not be created: %w", err)
	}

	p := &Publisher{
		conn:       c,
		logger:     c.logger,
		streamName: streamName,
	}
	return p, nil
}

// Publisher is a NATS publisher that publishes to a NATS stream.
type Publisher struct {
	conn       *Connection
	streamName string
	logger     *slog.Logger
}

// Publish publishes the message (data) to the given subject.
func (p *Publisher) Publish(msg *Msg) error {
	if err := validateSubject(msg.Subject, p.streamName); err != nil {
		return err
	}

	err := p.conn.nats.PublishMsg(msg.toNATS(), msg.MsgID)
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

// return the number of replicas between 3 and 5
func validateReplicas(new, old int) int {
	if new < 1 {
		new = old
	}
	if new < 1 || new > 5 {
		return 3
	}
	return new
}
