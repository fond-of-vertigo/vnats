package vnats

import (
	"encoding/json"
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/nats-io/nats.go"
	"strings"
)

type Publisher interface {
	// Publish sends data to a specified subject to a streamInfo.
	// Each message has a msgID for de-duplication relative to
	// the duplication-time-window of each streamInfo.
	Publish(subject string, data interface{}, msgID string) error
}

type publisher struct {
	conn *connection
	log  logger.Logger
}

func validateSubject(subject string) error {
	if subject == "" {
		return fmt.Errorf("subject cannot be empty")
	}
	if strings.HasPrefix(subject, ".") {
		return fmt.Errorf("subject needs to start with `STREAM_NAME.`")
	}
	return nil
}

// Publish sends data to a specified subject to a streamInfo.
// Each message has a msgID for de-duplication relative to
// the duplication-time-window of each streamInfo.
func (p *publisher) Publish(subject string, data interface{}, msgID string) error {
	if err := validateSubject(subject); err != nil {
		return err
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("message with msg-ID: %s @ %s could not be published: %w", msgID, subject, err)
	}

	p.log.Debugf("Publish message with msg-ID: %s @ %s\n", msgID, subject)
	msg := NewMessage(subject, dataBytes)
	if err = p.conn.nats.PublishMsg(msg, msgID); err != nil {
		return fmt.Errorf("message with msg-ID: %s @ %s could not be published: %w", msgID, subject, err)
	}
	return nil
}

func makePublisher(conn *connection, streamName string, logger logger.Logger) (*publisher, error) {
	_, err := conn.nats.GetOrAddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(conn.nats.Servers()),
		Duplicates: defaultDuplicationWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("publisher could not be created: %w", err)
	}

	p := &publisher{
		conn: conn,
		log:  logger,
	}
	return p, nil
}
