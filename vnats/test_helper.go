package vnats

import (
	"fmt"
	"github.com/fond-of/logging.go/logger"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
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
