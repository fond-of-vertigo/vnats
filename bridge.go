package vnats

import (
	"fmt"
	"log/slog"
	"strings"

	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type natsBridge struct {
	connection       *nats.Conn
	jetStreamContext nats.JetStreamContext
	logger           *slog.Logger
}

func newNATSBridge(servers []string, logger *slog.Logger) (*natsBridge, error) {
	nb := &natsBridge{
		logger: logger,
	}

	var err error
	url := strings.Join(servers, ",")

	nb.connection, err = nats.Connect(url,
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Error("Got disconnected", slog.String("error", err.Error()))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Error("Got reconnected to!", slog.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Error("Connection closed", slog.String("error", nc.LastError().Error()))
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
		b.logger.Info("Stream not found, about to add stream.", slog.String("name", streamConfig.Name))

		_, err = b.jetStreamContext.AddStream(streamConfig)
		if err != nil {
			return fmt.Errorf("streamInfo %s could not be added: %w", streamConfig.Name, err)
		}
		b.logger.Info("Added new NATS streamInfo", slog.String("name", streamConfig.Name))
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
