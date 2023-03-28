package vnats

import (
	"errors"
	"fmt"
	"strings"

	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type natsBridge struct {
	connection       *nats.Conn
	jetStreamContext nats.JetStreamContext
	log              Log
}

func newNATSBridge(servers []string, log Log) (*natsBridge, error) {
	nb := &natsBridge{
		log: log,
	}

	var err error
	url := strings.Join(servers, ",")

	nb.connection, err = nats.Connect(url,
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log("Got disconnected: %v\n", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log("Connection closed: %v\n", nc.LastError())
		}))
	if err != nil {
		return nil, fmt.Errorf("could not make NATS Connection to %s: %w", url, err)
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
		c.log("Stream %s not found, trying to create...\n", streamConfig.Name)

		streamInfo, err = c.jetStreamContext.AddStream(streamConfig)
		if err != nil {
			return nil, fmt.Errorf("streamInfo %s could not be created: %w", streamConfig.Name, err)
		}
		c.log("created new NATS streamInfo %s\n", streamConfig.Name)
	}

	return streamInfo, nil
}

func (c *natsBridge) CreateSubscription(subject, consumerName string, mode SubscriptionMode) (*natsSubscription, error) {
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
	if errors.Is(err, nats.ErrConsumerNotFound) {
		c.log("Consumer %s not found, trying to create...\n", consumerConfig.Durable)
		return c.jetStreamContext.AddConsumer(streamName, consumerConfig)
	} else if err != nil {
		return nil, fmt.Errorf("NATS consumer could not be fetched: %w", err)
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

type natsSubscription struct {
	streamSubscription *nats.Subscription
}

func (s *natsSubscription) Fetch() (*nats.Msg, error) {
	messages, err := s.streamSubscription.Fetch(1)
	if err != nil {
		return nil, err
	}

	return messages[0], nil
}

func (s *natsSubscription) Unsubscribe() error {
	return s.streamSubscription.Unsubscribe()
}

func (s *natsSubscription) Drain() error {
	return s.streamSubscription.Drain()
}
