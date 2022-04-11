package vnats

import (
	"github.com/fond-of/logging.go/logger"
	"testing"
)

const integrationTestStreamName = "IntegrationTests"

var log = logger.New(logger.LvlDebug)

func TestConnection_NewPublisher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	conn := makeIntegrationTestConn(t, integrationTestStreamName, log)

	_, err := conn.NewPublisher(integrationTestStreamName)
	if err != nil {
		t.Errorf("Publisher could not be created: %v", err)
	}
}

type newSubscriberConfig struct {
	consumerName string
	subject      string
	mode         SubscriptionMode
}

var newSubscriberTestCases = []newSubscriberConfig{
	{"IntegrationTestConsumer", integrationTestStreamName + ".*", MultipleInstances},
	{"IntegrationTestConsumer", integrationTestStreamName + ".*", SingleInstanceMessagesInOrder},
	{"IntegrationTestConsumer", integrationTestStreamName + ".tests.*", MultipleInstances},
	{"IntegrationTestConsumer", integrationTestStreamName + ".tests.*", SingleInstanceMessagesInOrder},
}

func TestConnection_NewSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	for _, test := range newSubscriberTestCases {
		conn := makeIntegrationTestConn(t, integrationTestStreamName, log)

		_, err := conn.NewPublisher(integrationTestStreamName)
		if err != nil {
			t.Errorf("Publisher could not be created: %v", err)
		}
		_, err = conn.NewSubscriber(test.consumerName, test.subject, test.mode)
		if err != nil {
			t.Errorf("Subscriber could not be created: %v", err)
		}
	}

}
