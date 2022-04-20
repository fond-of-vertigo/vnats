package vnats

import (
	"errors"
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"os"
	"reflect"
	"testing"
	"time"
)

var testLogger = logger.New(logger.LvlDebug)

type testBridge struct {
	streamName     string
	sequenceNumber uint64
	wantData       []byte
	wantMessageID  string
}

func (b *testBridge) DeleteStream(_ string) error {
	return nil
}

func (b *testBridge) DeleteConsumers(_ string, _ string) error {
	return nil
}

func (b *testBridge) GetOrAddStream(_ *nats.StreamConfig) (*nats.StreamInfo, error) {
	return nil, nil

}

func (b *testBridge) Servers() []string {
	return nil
}

func (b *testBridge) PublishMsg(msg *nats.Msg, msgID string) error {
	testLogger.Debugf("%s\n", string(msg.Data))
	if diff := cmp.Diff(msg.Data, b.wantData); diff != "" {
		testLogger.Errorf(diff)
		return fmt.Errorf("wrong message found=%s (id=%s) want=%s (id=%s)", string(msg.Data), msgID, string(b.wantData), b.wantMessageID)
	}
	if msgID != b.wantMessageID {
		return fmt.Errorf("wrong message ID found=%s want=%s", msgID, b.wantMessageID)
	}
	return nil
}

func (b *testBridge) CreateSubscription(_ string, _ string, _ SubscriptionMode) (subscription, error) {
	return nil, nil
}

func (b *testBridge) Drain() error {
	return nil
}

func makeTestNATSBridge(streamName string, currentSequenceNumber uint64, wantData []byte, wantMessageID string) bridge {
	return &testBridge{
		streamName:     streamName,
		sequenceNumber: currentSequenceNumber,
		wantData:       wantData,
		wantMessageID:  wantMessageID,
	}
}

func makeTestConnection(streamName string, currentSequenceNumber uint64, wantData []byte, wantMessageID string, wantSubs []*subscriber) *connection {
	return &connection{
		nats:        makeTestNATSBridge(streamName, currentSequenceNumber, wantData, wantMessageID),
		log:         testLogger,
		subscribers: wantSubs,
	}
}

func createStream(b *natsBridge, streamName string) error {
	_, err := b.GetOrAddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(b.Servers()),
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	})
	return err
}

func deleteStream(b *natsBridge, streamName string) error {
	return b.jetStreamContext.DeleteStream(streamName)
}

func deleteConsumer(c *connection, b *natsBridge, streamName string) error {
	for _, sub := range c.subscribers {
		consumerName := sub.consumerName

		if err := sub.Unsubscribe(); err != nil {
			return err
		}

		if err := b.jetStreamContext.DeleteConsumer(streamName, consumerName); err != nil {
			return err
		}
	}
	return nil
}

func makeIntegrationTestConn(t *testing.T, streamName string, log logger.Logger) Connection {
	conn := &connection{
		log: log,
	}

	nb := &natsBridge{
		log: log,
	}

	var err error
	url := os.Getenv("NATS_SERVER_URL")
	if url == "" {
		t.Error("Env-Var `NATS_SERVER_URL` is empty!")
	}
	nb.connection, err = nats.Connect(url)
	if err != nil {
		t.Error(fmt.Errorf("could not make NATS connection to %s: %w", url, err))
	}

	nb.jetStreamContext, err = nb.connection.JetStream()
	if err != nil {
		t.Error(err)
	}

	conn.nats = nb

	if err := deleteConsumer(conn, nb, streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		t.Errorf("Could not delete consumers %s: %v.", streamName, err)
	}
	if err := deleteStream(nb, streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		t.Errorf("Could not delete stream %s: %v.", streamName, err)
	}
	if err := createStream(nb, streamName); err != nil {
		t.Errorf("Stream %s could not be created: %v", streamName, err)
	}
	return conn
}

func cmpStringSlicesIgnoreOrder(expectedMessages []string, receivedMessages []string) error {
	if len(expectedMessages) == 0 && len(receivedMessages) == 0 {
		return nil
	}
	for _, expectedMsg := range expectedMessages {
		for idx, foundMsg := range receivedMessages {
			if expectedMsg == foundMsg {
				receivedMessages[idx] = receivedMessages[len(receivedMessages)-1]
				receivedMessages = receivedMessages[:len(receivedMessages)-1]
			}
		}
	}

	if !reflect.DeepEqual(receivedMessages, []string{}) {
		return fmt.Errorf("more messages were received than published. Additional msgs: %v", receivedMessages)
	}
	return nil
}

func publishStringMessages(t *testing.T, conn Connection, subject string, publishMessages []string) {
	pub, err := conn.NewPublisher(NewPublisherArgs{
		StreamName: integrationTestStreamName,
	})
	if err != nil {
		t.Error(err)
	}
	for idx, msg := range publishMessages {
		if err := pub.Publish(&OutMsg{
			Subject: subject,
			MsgID:   fmt.Sprintf("msg-%d", idx),
			Data:    []byte(msg),
		}); err != nil {
			t.Error(err)
		}
	}
}

func retrieveStringMessages(sub Subscriber, expectedMessages []string) ([]string, error) {
	var receivedMessages []string
	done := make(chan bool)

	handler := func(msg InMsg) error {
		receivedMessages = append(receivedMessages, string(msg.Data()))

		if len(receivedMessages) == len(expectedMessages) {
			done <- true
		}
		return nil
	}

	if err := waitFinishMsgHandler(sub, handler, done); err != nil {
		return nil, err
	}
	return receivedMessages, nil
}

func waitFinishMsgHandler(sub Subscriber, handler MsgHandler, done chan bool) error {
	timeout := time.Millisecond * 200
	if err := sub.Subscribe(handler); err != nil {
		return err
	}

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return nil
	}
}

func createSubscriber(t *testing.T, conn Connection, consumerName string, subject string, mode SubscriptionMode) Subscriber {
	sub, err := conn.NewSubscriber(NewSubscriberArgs{
		ConsumerName: consumerName,
		Subject:      subject,
		Mode:         mode,
	})
	if err != nil {
		t.Error(err)
	}
	return sub
}
