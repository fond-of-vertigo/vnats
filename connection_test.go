package vnats

import (
	"testing"
)

const integrationTestStreamName = "IntegrationTests"

func TestConnection_NewPublisher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	conn := makeIntegrationTestConn(t, integrationTestStreamName)

	_, err := conn.NewPublisher(NewPublisherArgs{
		StreamName: integrationTestStreamName,
	})
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
	{"IntegrationTestConsumer", integrationTestStreamName + ".*", MultipleSubscribersAllowed},
	{"IntegrationTestConsumer", integrationTestStreamName + ".*", SingleSubscriberStrictMessageOrder},
	{"IntegrationTestConsumer", integrationTestStreamName + ".tests.*", MultipleSubscribersAllowed},
	{"IntegrationTestConsumer", integrationTestStreamName + ".tests.*", SingleSubscriberStrictMessageOrder},
}

func TestConnection_NewSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	for _, test := range newSubscriberTestCases {
		conn := makeIntegrationTestConn(t, integrationTestStreamName)

		_, err := conn.NewPublisher(NewPublisherArgs{
			StreamName: integrationTestStreamName,
		})
		if err != nil {
			t.Errorf("Publisher could not be created: %v", err)
		}
		_, err = conn.NewSubscriber(NewSubscriberArgs{
			ConsumerName: test.consumerName,
			Subject:      test.subject,
			Mode:         test.mode,
		})
		if err != nil {
			t.Errorf("Subscriber could not be created: %v", err)
		}
	}

}
