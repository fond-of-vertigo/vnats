package vnats

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type subscribeStringsConfig struct {
	publishMessages  []string
	expectedMessages []string
	mode             SubscriptionMode
	wantErr          bool
}

var subscriberTestCases = []subscribeStringsConfig{
	{[]string{}, []string{}, MultipleSubscribersAllowed, false},
	{[]string{}, []string{}, SingleSubscriberStrictMessageOrder, false},
	{[]string{"hello", "world"}, []string{"hello", "world"}, SingleSubscriberStrictMessageOrder, false},
	{[]string{"hello", "world"}, []string{"world", "hello"}, SingleSubscriberStrictMessageOrder, true},
	{[]string{"hello", "world"}, []string{"hello", "world"}, MultipleSubscribersAllowed, false},
	{[]string{"hello", "world"}, []string{"world", "hello"}, MultipleSubscribersAllowed, false},
	{[]string{"hello", "world"}, []string{"world"}, MultipleSubscribersAllowed, true},
	{[]string{"hello", "world"}, []string{"world"}, SingleSubscriberStrictMessageOrder, true},
}

func TestSubscriber_Subscribe_Strings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".PubSubTest.string"

	for _, test := range subscriberTestCases {
		conn := makeIntegrationTestConn(t, integrationTestStreamName, log)

		publishStringMessages(t, conn, subject, test.publishMessages)

		sub, err := conn.NewSubscriber(NewSubscriberArgs{
			ConsumerName: "TestConsumer",
			Subject:      subject,
			Encoding:     EncJSON,
			Mode:         test.mode,
		})
		if err != nil {
			t.Error(err)
		}

		receivedMessages, err := retrieveStringMessages(sub, test.expectedMessages)
		if err != nil {
			t.Error(err)
		}

		switch test.mode {
		case MultipleSubscribersAllowed:
			if err := cmpStringSlicesIgnoreOrder(test.expectedMessages, receivedMessages); err != nil && !test.wantErr {
				t.Error(err)
			} else if test.wantErr && err == nil {
				t.Error("Should fail, but no error was thrown!")
			}

		case SingleSubscriberStrictMessageOrder:
			if len(test.expectedMessages) == 0 && len(receivedMessages) == 0 {
				continue
			}
			equal := reflect.DeepEqual(receivedMessages, test.expectedMessages)
			if !equal && !test.wantErr {
				t.Errorf("Got %v, expected %v\n", receivedMessages, test.expectedMessages)
			} else if equal && test.wantErr {
				t.Error("Should fail, but no error was thrown!")
			}
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
}

func TestSubscriber_Subscribe_Struct(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".PubSubTest.struct"

	for _, test := range subscriberTestCases {
		conn := makeIntegrationTestConn(t, integrationTestStreamName, log)

		publishTestMessageStructMessages(t, conn, subject, test.publishMessages)

		sub, err := conn.NewSubscriber(NewSubscriberArgs{
			ConsumerName: "TestConsumer",
			Subject:      subject,
			Encoding:     EncJSON,
			Mode:         test.mode,
		})
		if err != nil {
			t.Error(err)
		}

		receivedMessages, err := retrieveTestMessageStructMessages(sub, test.expectedMessages)
		if err != nil {
			t.Error(err)
		}

		switch test.mode {
		case MultipleSubscribersAllowed:
			if err := cmpStringSlicesIgnoreOrder(test.expectedMessages, receivedMessages); err != nil && !test.wantErr {
				t.Error(err)
			} else if test.wantErr && err == nil {
				t.Error("Should fail, but no error was thrown!")
			}

		case SingleSubscriberStrictMessageOrder:
			if len(test.expectedMessages) == 0 && len(receivedMessages) == 0 {
				continue
			}
			equal := reflect.DeepEqual(receivedMessages, test.expectedMessages)
			if !equal && !test.wantErr {
				t.Errorf("Got %v, expected %v\n", receivedMessages, test.expectedMessages)
			} else if equal && test.wantErr {
				t.Error("Should fail, but no error was thrown!")
			}
		}

		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
}
func TestSubscriber_CallTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".subscribeTwice"
	conn := makeIntegrationTestConn(t, integrationTestStreamName, log)
	publishStringMessages(t, conn, subject, []string{})
	sub, err := conn.NewSubscriber(NewSubscriberArgs{
		ConsumerName: "TestConsumer",
		Subject:      subject,
	})
	if err != nil {
		t.Error(err)
	}
	handler := func(_ *interface{}) error { return nil }

	if err := sub.Subscribe(handler); err != nil {
		t.Error(err)
	}
	err = sub.Subscribe(handler)
	if err.Error() != "handler is already set, don't call Subscribe() multiple times" {
		t.Errorf("Error expeceted, but not received! Err: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
}

type restDownConfig struct {
	mode                    SubscriptionMode
	minCallFirstMsg         int
	minCallSecondMsg        int
	maxCallSecondMsg        int
	waitUntilCheckCallCount time.Duration
}

var restDownTestCases = []restDownConfig{
	{SingleSubscriberStrictMessageOrder, 2, 0, 0, defaultNakDelay * 2},
	{MultipleSubscribersAllowed, 2, 2, 3, defaultNakDelay * 2},
}

func TestSubscriberAlwaysFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	subject := integrationTestStreamName + ".subscriberAlwaysFails"
	for _, test := range restDownTestCases {
		conn := makeIntegrationTestConn(t, integrationTestStreamName, log)
		publishStringMessages(t, conn, subject, []string{"hello", "world"})
		sub := createSubscriber(t, conn, "TestSubscriberAlwaysFails", subject, test.mode)

		callCountHello, callCountWorld := 0, 0

		handler := func(msg string) error {
			switch msg {
			case "hello":
				callCountHello += 1
			case "world":
				callCountWorld += 1
			}
			return fmt.Errorf("REST-Endpoint is down, retry later")
		}

		if err := sub.Subscribe(handler); err != nil {
			t.Error(err)
		}

		time.Sleep(test.waitUntilCheckCallCount)

		if callCountHello < test.minCallFirstMsg {
			t.Errorf("First message was not retried more than %v in %v.", test.minCallSecondMsg, test.waitUntilCheckCallCount)
		}
		if callCountWorld < test.minCallSecondMsg || callCountWorld > test.maxCallSecondMsg {
			t.Errorf("Second message was not called the correct amount: %v < %v < %v in %v.", test.minCallSecondMsg, callCountWorld, test.maxCallSecondMsg, test.waitUntilCheckCallCount)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
}

type subscriberConfig struct {
	mode              SubscriptionMode
	alwaysFail        bool
	minSuccessfulMsgs int
	minFailedMsgs     int
}
type multipleSubscriberConfig struct {
	subscribers             []subscriberConfig
	waitUntilCheckCallCount time.Duration
	wantErr                 bool
}

var multipleSubscriberTestCases = []multipleSubscriberConfig{
	{
		subscribers: []subscriberConfig{
			{
				mode:              MultipleSubscribersAllowed,
				alwaysFail:        true,
				minSuccessfulMsgs: 0,
				minFailedMsgs:     1,
			}, {
				mode:              MultipleSubscribersAllowed,
				alwaysFail:        false,
				minSuccessfulMsgs: 6,
				minFailedMsgs:     0,
			},
		},
		waitUntilCheckCallCount: defaultNakDelay,
		wantErr:                 false,
	},
	{
		subscribers: []subscriberConfig{
			{
				mode:              MultipleSubscribersAllowed,
				alwaysFail:        false,
				minSuccessfulMsgs: 3,
				minFailedMsgs:     0,
			}, {
				mode:              MultipleSubscribersAllowed,
				alwaysFail:        false,
				minSuccessfulMsgs: 3,
				minFailedMsgs:     0,
			},
		},
		waitUntilCheckCallCount: defaultNakDelay,
		wantErr:                 false,
	},
}

type subscriptionState struct {
	subscriber     Subscriber
	SuccessfulMsgs int
	FailedMsgs     int
}

func TestSubscriberMultiple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".multipleSubscriber"

	for _, test := range multipleSubscriberTestCases {
		var s []subscriptionState

		conn := makeIntegrationTestConn(t, integrationTestStreamName, log)

		for idx, subConfig := range test.subscribers {
			sub := createSubscriber(t, conn, "TestSubscriberAlwaysFails", subject, subConfig.mode)
			s = append(s, subscriptionState{subscriber: sub})

			handler := func(msg string) error {
				if subConfig.alwaysFail {
					s[idx].FailedMsgs += 1
					t.Logf("Subscriber %v: Failed msg handeling", idx)
					return fmt.Errorf("msg handleing failed")
				}
				t.Logf("Subscriber %v: Successful msg handeling", idx)
				s[idx].SuccessfulMsgs += 1
				return nil
			}

			if err := s[idx].subscriber.Subscribe(handler); err != nil && !test.wantErr {
				t.Error(err)
			}
		}

		publishStringMessages(t, conn, subject, []string{"hello", "world", "this", "is", "robot", "z"})

		time.Sleep(test.waitUntilCheckCallCount)

		for idx, subState := range s {
			if subState.FailedMsgs < test.subscribers[idx].minFailedMsgs {
				t.Errorf("Subscriber %d: Too less messages failed %d < %d", idx, subState.FailedMsgs, test.subscribers[idx].minFailedMsgs)
			}
			if subState.SuccessfulMsgs < test.subscribers[idx].minSuccessfulMsgs {
				t.Errorf("Subscriber %d: Too less messages were successful %d < %d", idx, subState.SuccessfulMsgs, test.subscribers[idx].minSuccessfulMsgs)
			}
		}
	}
}
