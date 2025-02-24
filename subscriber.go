package vnats

import (
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// MustMakeSubscriber creates a new Subscriber that subscribes to a NATS stream.
func (c *Connection) MustMakeSubscriber(args SubscriberArgs, handler MsgHandler) *Subscriber {
	sub, err := c.NewSubscriber(args, handler)
	if err != nil {
		panic(err)
	}
	return sub
}

// NewSubscriber creates a new Subscriber that subscribes to a NATS stream.
func (c *Connection) NewSubscriber(args SubscriberArgs, handler MsgHandler) (*Subscriber, error) {
	subscription, err := c.nats.Subscribe(args.Subject, args.ConsumerName, args.Mode)
	if err != nil {
		return nil, fmt.Errorf("subscriber could not be created: %w", err)
	}

	// set the default NakDelay if not set
	if args.NakDelay == 0 {
		args.NakDelay = defaultNakDelay
	}

	sub := &Subscriber{
		conn:         c,
		subscription: subscription,
		logger:       c.logger,
		consumerName: args.ConsumerName,
		nakDelay:     args.NakDelay,
		handler:      handler,
	}

	c.subscribers = append(c.subscribers, sub)
	return sub, nil
}

// MsgHandler is the type of function the Subscriber has to implement to process an incoming message.
type MsgHandler func(msg Msg) error

// Subscriber subscribes to a NATS consumer and pulls messages to handle by MsgHandler.
type Subscriber struct {
	conn         *Connection
	subscription *nats.Subscription
	logger       *slog.Logger
	consumerName string
	handler      MsgHandler
	nakDelay     time.Duration
	started      atomic.Bool
}

// Start subscribes to the NATS consumer and starts a go-routine that handles pulled messages.
// Important: Start should be called only once and can't be called after Stop.
func (s *Subscriber) Start() {
	if !s.started.CompareAndSwap(false, true) {
		s.logger.Warn("Subscriber already started, ignoring method call", slog.String("name", s.consumerName))
		return
	}

	go func() {
		for {
			err := s.processMessages()
			if err != nil {
				s.logger.Info("stopping nats message handler", slog.String("name", s.consumerName), slog.String("error", err.Error()))
				return
			}
		}
	}()
}

// Stop unsubscribes the consumer from the NATS stream.
// Important: After calling Stop the subscription is closed and can't be started again.
func (s *Subscriber) Stop() {
	if err := s.subscription.Unsubscribe(); err != nil {
		s.logger.Error("failed to unsubscribe consumer", slog.String("name", s.consumerName), slog.String("error", err.Error()))
		return
	}

	s.logger.Info("Unsubscribed consumer", slog.String("name", s.consumerName))
}

func (s *Subscriber) processMessages() error {
	natsMsgs, err := s.subscription.Fetch(1) // Fetch only one msg at once to keep the order
	if errors.Is(err, nats.ErrTimeout) {     // ErrTimeout is expected/ no new messages, so we don't log it
		return nil
	} else if errors.Is(err, nats.ErrBadSubscription) { // Subscription was closed
		return err
	} else if err != nil {
		s.logger.Error("Failed to receive msg", slog.String("error", err.Error()))
		return nil
	}

	msg := makeMsg(natsMsgs[0])
	if err = s.handler(msg); err != nil {
		s.logger.Error("Message handle error, will be NAKed", slog.String("error", err.Error()))
		if err := natsMsgs[0].NakWithDelay(s.nakDelay); err != nil {
			s.logger.Error("natsMsg.Nak() failed", slog.String("error", err.Error()))
		}
		return nil
	}

	if err = natsMsgs[0].Ack(); err != nil {
		s.logger.Error("natsMsg.Ack() failed:", slog.String("error", err.Error()))
	}

	return nil
}
