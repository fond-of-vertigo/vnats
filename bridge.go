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
	log              LogFunc
}

func newNATSBridge(servers []string, log LogFunc) (*natsBridge, error) {
	nb := &natsBridge{
		log: log,
	}

	var err error
	url := strings.Join(servers, ",")

	nb.connection, err = nats.Connect(url,
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log(LogLevelInfo, "Got disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log(LogLevelInfo, "Got reconnected to %v!", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log(LogLevelInfo, "Connection closed: %v", nc.LastError())
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

func (b *natsBridge) PublishMsg(msg *nats.Msg, msgID string) error {
	_, err := b.jetStreamContext.PublishMsg(msg, nats.MsgId(msgID))
	return err
}

func (b *natsBridge) FetchOrAddStream(streamConfig *nats.StreamConfig) (*nats.StreamInfo, error) {
	streamInfo, err := b.jetStreamContext.StreamInfo(streamConfig.Name)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			return nil, fmt.Errorf("NATS streamInfo-info could not be fetched: %w", err)
		}
		b.log(LogLevelInfo, "Stream %s not found, about to add stream.", streamConfig.Name)

		streamInfo, err = b.jetStreamContext.AddStream(streamConfig)
		if err != nil {
			return nil, fmt.Errorf("streamInfo %s could not be added: %w", streamConfig.Name, err)
		}
		b.log(LogLevelInfo, "Added new NATS streamInfo %s", streamConfig.Name)
	}

	return streamInfo, nil
}

func (b *natsBridge) CreateSubscription(subject, consumerName string, mode SubscriptionMode) (*nats.Subscription, error) {
	streamName := strings.Split(subject, ".")[0]
	config := &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
		AckWait:   defaultAckWait,
	}

	patchConsumerConfig(config, mode)

	if _, err := b.fetchOrAddConsumer(streamName, config); err != nil {
		return nil, err
	}

	return b.jetStreamContext.PullSubscribe(subject, consumerName, nats.Bind(streamName, consumerName))
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

func (b *natsBridge) fetchOrAddConsumer(streamName string, consumerConfig *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	ci, err := b.jetStreamContext.ConsumerInfo(streamName, consumerConfig.Durable)
	if errors.Is(err, nats.ErrConsumerNotFound) {
		b.log(LogLevelInfo, "Consumer %s not found, about to add consumer.", consumerConfig.Durable)
		if ci, err = b.jetStreamContext.AddConsumer(streamName, consumerConfig); err != nil {
			return nil, fmt.Errorf("NATS consumer could not be fetched: %w", err)
		}
		b.log(LogLevelInfo, "Created new NATS consumer %s", consumerConfig.Durable)
		return ci, nil
	} else if err != nil {
		return nil, fmt.Errorf("consumer %s could not be fetched: %w", consumerConfig.Durable, err)
	}

	if ci.Config.MaxAckPending != consumerConfig.MaxAckPending {
		b.log(LogLevelError, "Consumer %s SubscriptionMode has changed. Use the existing SubscriptionMode=%v or delete consumer.",
			consumerConfig.Durable, SubscriptionMode(ci.Config.MaxAckPending))
		return nil, fmt.Errorf("stream consumer SubscriptionMode %v does not match with consumerConfig", SubscriptionMode(ci.Config.MaxAckPending))
	}

	return ci, nil
}

func (b *natsBridge) Servers() []string {
	return b.connection.Servers()
}

func (b *natsBridge) Drain() error {
	return b.connection.Drain()
}
