package vnats

import (
	"fmt"
	"github.com/fond-of-vertigo/logger"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"strings"
)

// bridge is required to use a mock for the nats functions in unit tests
type bridge interface {
	// GetOrAddStream returns a *nats.StreamInfo and for the given streamInfo name.
	// It adds a new streamInfo if it does not exist.
	GetOrAddStream(streamConfig *nats.StreamConfig) (*nats.StreamInfo, error)

	// CreateSubscription creates a natsSubscription, that can fetch messages from a specified subject.
	// The first token, seperated by dots, of a subject will be interpreted as the streamName.
	CreateSubscription(subject string, consumerName string, mode SubscriptionMode) (subscription, error)

	// Servers returns the list of NATS servers.
	Servers() []string

	// PublishMsg publishes a message with a context-dependent msgID to a subject.
	PublishMsg(msg *nats.Msg, msgID string) error

	// Drain will put a connection into a drain state. All subscriptions will
	// immediately be put into a drain state. Upon completion, the publishers
	// will be drained and can not publish any additional messages. Upon draining
	// of the publishers, the connection will be closed.
	//
	// See notes for nats.Conn.Drain
	Drain() error
}

type natsBridge struct {
	connection       *nats.Conn
	jetStreamContext nats.JetStreamContext
	log              logger.Logger
}

func makeNATSBridge(servers []string, log logger.Logger) (bridge, error) {
	nb := &natsBridge{
		log: log,
	}

	var err error
	url := strings.Join(servers, ",")

	nb.connection, err = nats.Connect(url,
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Errorf("Got disconnected: %v\n", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Errorf("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Errorf("Connection closed: %v\n", nc.LastError())
		}))
	if err != nil {
		return nil, fmt.Errorf("could not make NATS connection to %s: %w", url, err)
	}

	nb.jetStreamContext, err = nb.connection.JetStream()
	if err != nil {
		return nil, err
	}

	return nb, nil
}

func (c *natsBridge) PublishMsg(msg *nats.Msg, msgID string) error {
	_, err := c.jetStreamContext.PublishMsg(msg, nats.MsgId(msgID))
	return err
}

func (c *natsBridge) GetOrAddStream(streamConfig *nats.StreamConfig) (*nats.StreamInfo, error) {
	streamInfo, err := c.jetStreamContext.StreamInfo(streamConfig.Name)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			return nil, fmt.Errorf("NATS streamInfo-info could not be fetched: %w", err)
		}
		c.log.Debugf("Stream %s not found, trying to create...\n", streamConfig.Name)

		streamInfo, err = c.jetStreamContext.AddStream(streamConfig)
		if err != nil {
			return nil, fmt.Errorf("streamInfo %s could not be created: %w", streamConfig.Name, err)
		}
		c.log.Debugf("created new NATS streamInfo %s\n", streamConfig.Name)
	}

	return streamInfo, nil
}

func (c *natsBridge) CreateSubscription(subject string, consumerName string, mode SubscriptionMode) (subscription, error) {
	streamName := strings.Split(subject, ".")[0]
	config := &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
		AckWait:   defaultAckWait,
	}

	patchConsumerConfig(config, mode)

	if _, err := c.getOrAddConsumer(streamName, config); err != nil {
		return nil, err
	}

	sub, err := c.jetStreamContext.PullSubscribe(subject, consumerName, nats.Bind(streamName, consumerName))
	if err != nil {
		return nil, err
	}

	return &natsSubscription{streamSubscription: sub}, nil
}

func patchConsumerConfig(config *nats.ConsumerConfig, mode SubscriptionMode) {
	switch mode {
	case MultipleSubscribersAllowed:
		config.MaxAckPending = natsServer.JsDefaultMaxAckPending
	case SingleSubscriberStrictMessageOrder:
		config.MaxAckPending = 1
	default:
		config.MaxAckPending = natsServer.JsDefaultMaxAckPending
	}
}

func (c *natsBridge) getOrAddConsumer(streamName string, consumerConfig *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	ci, err := c.jetStreamContext.ConsumerInfo(streamName, consumerConfig.Durable)
	if err != nil {
		if !strings.Contains(err.Error(), "consumer not found") {
			return nil, err
		}

		ci, err = c.jetStreamContext.AddConsumer(streamName, consumerConfig)
		if err != nil {
			return nil, fmt.Errorf("consumer %s could not be added to stream %s: %w", consumerConfig.Durable, streamName, err)
		}

		c.log.Debugf("Consumer %s for stream %s created at %s. %d messages pending, #%d ack pending", ci.Name, streamName, ci.Created, ci.NumPending, ci.NumAckPending)
		return ci, nil
	}

	if ci.Config.MaxAckPending != consumerConfig.MaxAckPending {
		return nil, fmt.Errorf("consumer %s SubscriptionMode has changed. "+
			"Please use the existing SubscriptionMode=%v or delete consumer", consumerConfig.Durable, SubscriptionMode(ci.Config.MaxAckPending))
	}

	return ci, nil
}

func (c *natsBridge) Servers() []string {
	return c.connection.Servers()
}

func (c *natsBridge) Drain() error {
	return c.connection.Drain()
}
