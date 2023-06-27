package vnats

import (
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
			log("Got disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log("Got reconnected to %v!", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log("Connection closed: %v", nc.LastError())
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

func (b *natsBridge) EnsureStreamExists(streamConfig *nats.StreamConfig) error {
	if _, err := b.jetStreamContext.StreamInfo(streamConfig.Name); err != nil {
		if err != nats.ErrStreamNotFound {
			return fmt.Errorf("NATS streamInfo-info could not be fetched: %w", err)
		}
		b.log("Stream %s not found, about to add stream.", streamConfig.Name)

		_, err = b.jetStreamContext.AddStream(streamConfig)
		if err != nil {
			return fmt.Errorf("streamInfo %s could not be added: %w", streamConfig.Name, err)
		}
		b.log("Added new NATS streamInfo %s", streamConfig.Name)
	}
	return nil
}

func (b *natsBridge) Subscribe(subject, consumerName string, mode SubscriptionMode) (*nats.Subscription, error) {
	var maxAckPending int
	switch mode {
	case MultipleSubscribersAllowed:
		maxAckPending = natsServer.JsDefaultMaxAckPending
	case SingleSubscriberStrictMessageOrder:
		maxAckPending = 1
	default:
		maxAckPending = natsServer.JsDefaultMaxAckPending
	}

	return b.jetStreamContext.PullSubscribe(subject, consumerName,
		nats.AckExplicit(),
		nats.MaxAckPending(maxAckPending),
		nats.AckWait(defaultAckWait),
	)
}

func (b *natsBridge) Servers() []string {
	return b.connection.Servers()
}

func (b *natsBridge) Drain() error {
	return b.connection.Drain()
}
