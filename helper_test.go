package vnats

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
)

const (
	defaultServerURL          = "nats://127.0.0.1:4222"
	integrationTestStreamName = "IntegrationTests"
	testNakDelay              = time.Second * 3
)

type testBridge struct {
	testing.TB
	streamName     string
	sequenceNumber uint64
	wantData       []byte
	wantMessageID  string
}

func (b *testBridge) EnsureStreamExists(_ *nats.StreamConfig) error {
	return nil
}

func (b *testBridge) DeleteStream(_ string) error {
	return nil
}

func (b *testBridge) DeleteConsumers(_, _ string) error {
	return nil
}

func (b *testBridge) Servers() []string {
	return nil
}

func (b *testBridge) PublishMsg(msg *nats.Msg, msgID string) error {
	b.Logf("%s", string(msg.Data))
	if diff := cmp.Diff(msg.Data, b.wantData); diff != "" {
		err := fmt.Errorf("wrong message found=%s (id=%s) want=%s (id=%s)", string(msg.Data), msgID, b.wantData, b.wantMessageID)
		b.Fatal(err, diff)
	}
	if msgID != b.wantMessageID {
		b.Fatalf("wrong message ID found=%s want=%s", msgID, b.wantMessageID)
	}
	return nil
}

func (b *testBridge) Subscribe(_, _ string, _ SubscriptionMode) (*nats.Subscription, error) {
	return nil, nil
}

func (b *testBridge) Drain() error {
	return nil
}

func makeTestNATSBridge(t testing.TB, streamName string, currentSequenceNumber uint64, wantData []byte, wantMessageID string) bridge {
	return &testBridge{
		TB:             t,
		streamName:     streamName,
		sequenceNumber: currentSequenceNumber,
		wantData:       wantData,
		wantMessageID:  wantMessageID,
	}
}

func makeTestConnection(t *testing.T, streamName string, currentSequenceNumber uint64, wantData []byte, wantMessageID string, wantSubs []*Subscriber) *Connection {
	return &Connection{
		nats:        makeTestNATSBridge(t, streamName, currentSequenceNumber, wantData, wantMessageID),
		logger:      slog.Default(),
		subscribers: wantSubs,
	}
}

func createStream(b *natsBridge, streamName string) error {
	return b.EnsureStreamExists(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   len(b.Servers()),
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	})
}

func deleteStream(b *natsBridge, streamName string) error {
	return b.jetStreamContext.DeleteStream(streamName)
}

func deleteConsumer(c *Connection, b *natsBridge, streamName string) error {
	for _, sub := range c.subscribers {
		consumerName := sub.consumerName

		sub.Stop()

		if err := b.jetStreamContext.DeleteConsumer(streamName, consumerName); err != nil {
			return err
		}
	}
	return nil
}

func makeIntegrationTestConn(t *testing.T) *Connection {
	conn := &Connection{
		logger: slog.Default(),
	}

	nb := &natsBridge{
		logger: slog.Default(),
	}

	var err error
	url := os.Getenv("NATS_SERVER_URL")
	if url == "" {
		url = defaultServerURL
	}
	nb.connection, err = nats.Connect(url)
	if err != nil {
		t.Errorf("could not make NATS Connection to %s: %v", url, err)
	}

	nb.jetStreamContext, err = nb.connection.JetStream()
	if err != nil {
		t.Error(err)
	}

	conn.nats = nb

	if err := deleteConsumer(conn, nb, integrationTestStreamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		t.Errorf("Could not delete consumers %s: %v.", integrationTestStreamName, err)
	}
	if err := deleteStream(nb, integrationTestStreamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		t.Errorf("Could not delete stream %s: %v.", integrationTestStreamName, err)
	}
	if err := createStream(nb, integrationTestStreamName); err != nil {
		t.Errorf("Stream %s could not be created: %v", integrationTestStreamName, err)
	}
	return conn
}

func cmpStringSlicesIgnoreOrder(expectedMessages, receivedMessages []string) error {
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

func publishManyMessages(t *testing.T, conn *Connection, subject string, messageCount int) {
	var messages []string
	for i := 0; i < messageCount; i++ {
		messages = append(messages, fmt.Sprintf("msg-%d", i))
	}

	publishStringMessages(t, conn, subject, messages)
}

func publishStringMessages(t *testing.T, conn *Connection, subject string, publishMessages []string) {
	pub, err := conn.NewPublisher(PublisherArgs{
		StreamName: integrationTestStreamName,
	})
	if err != nil {
		t.Error(err)
	}
	for idx, msg := range publishMessages {
		if err := pub.Publish(&Msg{
			Subject: subject,
			MsgID:   fmt.Sprintf("msg-%d", idx),
			Data:    []byte(msg),
		}); err != nil {
			t.Error(err)
		}
	}
}

func publishTestMessageStructMessages(t *testing.T, conn *Connection, subject string, publishMessages []string) {
	pub, err := conn.NewPublisher(PublisherArgs{
		StreamName: integrationTestStreamName,
	})
	if err != nil {
		t.Error(err)
	}

	for idx, msg := range publishMessages {
		dataAsBytes, err := json.Marshal(testMessagePayload{Message: msg})
		if err != nil {
			t.Error(err)
		}

		if err := pub.Publish(&Msg{
			Subject: subject,
			MsgID:   fmt.Sprintf("msg-%d", idx),
			Data:    dataAsBytes,
		}); err != nil {
			t.Error(err)
		}
	}
}

func retrieveStringMessages(sub *Subscriber, expectedMessages []string) []string {
	receivedMessages := []string{}
	done := make(chan bool)

	sub.handler = func(msg Msg) error {
		receivedMessages = append(receivedMessages, string(msg.Data))

		if len(receivedMessages) == len(expectedMessages) {
			done <- true
		}
		return nil
	}

	waitFinishMsgHandler(sub, done)

	return receivedMessages
}

func retrieveTestMessageStructMessages(sub *Subscriber, expectedMessages []string) []string {
	var receivedMessages []string
	done := make(chan bool)

	sub.handler = func(msg Msg) error {
		var data testMessagePayload
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		receivedMessages = append(receivedMessages, data.Message)

		if len(receivedMessages) == len(expectedMessages) {
			done <- true
		}
		return nil
	}

	waitFinishMsgHandler(sub, done)

	return receivedMessages
}

func waitFinishMsgHandler(sub *Subscriber, done chan bool) {
	sub.Start()

	select {
	case <-done:
		return
	case <-time.After(time.Millisecond * 200):
		return
	}
}

func createSubscriber(t *testing.T, conn *Connection, consumerName, subject string, mode SubscriptionMode, handler MsgHandler) *Subscriber {
	sub, err := conn.NewSubscriber(SubscriberArgs{
		ConsumerName: consumerName,
		Subject:      subject,
		Mode:         mode,
		NakDelay:     testNakDelay,
	}, handler)
	if err != nil {
		t.Error(err)
	}
	return sub
}

func nopMsgHandler(_ Msg) error {
	return nil
}
