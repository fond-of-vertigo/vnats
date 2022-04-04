package vnats

import (
	"github.com/fond-of/logging.go/logger"
	"testing"
)

var log = logger.New(logger.LvlDebug)

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	_, err := Connect([]string{"127.0.0.1:4222"}, log)
	if err != nil {
		t.Errorf("NATS connection could not be established: %v", err)
	}
}

func TestConnection_NewPublisher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	conn, err := Connect([]string{"127.0.0.1:4222"}, log)
	if err != nil {
		t.Errorf("NATS connection could not be established: %v", err)
	}

	_, err = conn.NewPublisher("IntegrationTests")
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
	{"IntegrationTestConsumer", "IntegrationTests.*", MultipleInstances},
	{"IntegrationTestConsumer", "IntegrationTests.*", SingleInstanceMessagesInOrder},
	{"IntegrationTestConsumer", "IntegrationTests.tests.*", MultipleInstances},
	{"IntegrationTestConsumer", "IntegrationTests.tests.*", SingleInstanceMessagesInOrder},
}

func TestConnection_NewSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	conn, err := Connect([]string{"127.0.0.1:4222"}, log)
	if err != nil {
		t.Errorf("NATS connection could not be established: %v", err)
	}

	for _, test := range newSubscriberTestCases {
		_, err = conn.NewSubscriber(test.consumerName, test.subject, test.mode)
		if err != nil {
			t.Errorf("Subscriber could not be created: %v", err)
		}
	}

}
