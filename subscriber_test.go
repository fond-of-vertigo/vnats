package vnats

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type subscribeStringsConfig struct {
	name             string
	publishMessages  []string
	expectedMessages []string
	mode             SubscriptionMode
	wantErr          bool
}

func TestSubscriber_Subscribe_Strings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	tests := []subscribeStringsConfig{
		{
			name:             "Publish in mode: SingleSubscriberStrictMessageOrder empty payload",
			publishMessages:  []string{},
			expectedMessages: []string{},
			mode:             MultipleSubscribersAllowed,
			wantErr:          false,
		},
		{
			name:             "Publish in mode: SingleSubscriberStrictMessageOrder empty payload",
			publishMessages:  []string{},
			expectedMessages: []string{},
			mode:             SingleSubscriberStrictMessageOrder,
			wantErr:          false,
		},
		{
			name:             "Publish in mode: SingleSubscriberStrictMessageOrder, no error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"hello", "world"},
			mode:             SingleSubscriberStrictMessageOrder,
			wantErr:          false,
		},
		{
			name:             "Publish in mode: SingleSubscriberStrictMessageOrder, wrong msg order, error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"world", "hello"},
			mode:             SingleSubscriberStrictMessageOrder,
			wantErr:          true,
		},
		{
			name:             "Publish in mode: MultipleSubscribersAllowed, no error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"hello", "world"},
			mode:             MultipleSubscribersAllowed,
			wantErr:          false,
		},
		{
			name:             "Publish in mode: MultipleSubscribersAllowed with ignored order, no error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"world", "hello"},
			mode:             MultipleSubscribersAllowed,
			wantErr:          false,
		},
		{
			name:             "Publish in mode: MultipleSubscribersAllowed wrong received message, error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"world"},
			mode:             MultipleSubscribersAllowed,
			wantErr:          true,
		},
		{
			name:             "Publish in mode: SingleSubscriberStrictMessageOrder wrong received message, error expected",
			publishMessages:  []string{"hello", "world"},
			expectedMessages: []string{"world"},
			mode:             SingleSubscriberStrictMessageOrder,
			wantErr:          true,
		},
	}
	subject := integrationTestStreamName + ".PubSubTest.string"

	dataTypeValidators := []func(t *testing.T, conn *Connection, sub *Subscriber, config subscribeStringsConfig) error{
		subscriberStringTest,
		subscriberStructTest,
	}
	for validatorIdx, typeValidation := range dataTypeValidators {
		t.Logf("Running datatype-validator #%d", validatorIdx)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				conn := makeIntegrationTestConn(t)

				sub, err := conn.CreateSubscriber(CreateSubscriberArgs{
					ConsumerName: "TestConsumer",
					Subject:      subject,
					Mode:         tt.mode,
				})
				if err != nil {
					t.Error(err)
				}

				if err := typeValidation(t, conn, sub, tt); err != nil {
					t.Error(err)
				}

				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

func subscriberStringTest(t *testing.T, conn *Connection, sub *Subscriber, config subscribeStringsConfig) error {
	subject := integrationTestStreamName + ".PubSubTest.string"
	publishStringMessages(t, conn, subject, config.publishMessages)

	receivedMessages, err := retrieveStringMessages(sub, config.expectedMessages)
	if err != nil {
		t.Error(err)
	}

	switch config.mode {
	case MultipleSubscribersAllowed:
		if err := cmpStringSlicesIgnoreOrder(config.expectedMessages, receivedMessages); err != nil && !config.wantErr {
			return err
		} else if config.wantErr && err == nil {
			return fmt.Errorf("should fail, but no error was thrown")
		}

	case SingleSubscriberStrictMessageOrder:
		if len(config.expectedMessages) == 0 && len(receivedMessages) == 0 {
			return nil
		}
		equal := reflect.DeepEqual(receivedMessages, config.expectedMessages)
		if !equal && !config.wantErr {
			t.Errorf("Got %v, expected %v\n", receivedMessages, config.expectedMessages)
		} else if equal && config.wantErr {
			return fmt.Errorf("should fail, but no error was thrown")
		}
	}
	return nil
}

func subscriberStructTest(t *testing.T, conn *Connection, sub *Subscriber, config subscribeStringsConfig) error {
	subject := integrationTestStreamName + ".PubSubTest.string"
	publishTestMessageStructMessages(t, conn, subject, config.publishMessages)

	receivedMessages, err := retrieveTestMessageStructMessages(sub, config.expectedMessages)
	if err != nil {
		t.Error(err)
	}

	switch config.mode {
	case MultipleSubscribersAllowed:
		if err := cmpStringSlicesIgnoreOrder(config.expectedMessages, receivedMessages); err != nil && !config.wantErr {
			return err
		} else if config.wantErr && err == nil {
			return fmt.Errorf("should fail, but no error was thrown")
		}

	case SingleSubscriberStrictMessageOrder:
		if len(config.expectedMessages) == 0 && len(receivedMessages) == 0 {
			return nil
		}
		equal := reflect.DeepEqual(receivedMessages, config.expectedMessages)
		if !equal && !config.wantErr {
			t.Errorf("Got %v, expected %v\n", receivedMessages, config.expectedMessages)
		} else if equal && config.wantErr {
			return fmt.Errorf("should fail, but no error was thrown")
		}
	}
	return nil
}

func TestSubscriber_CallTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".subscribeTwice"
	conn := makeIntegrationTestConn(t)
	publishStringMessages(t, conn, subject, []string{})
	sub, err := conn.CreateSubscriber(CreateSubscriberArgs{
		ConsumerName: "TestConsumer",
		Subject:      subject,
	})
	if err != nil {
		t.Error(err)
	}
	handler := func(_ Msg) error { return nil }

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

func TestSubscriberAlwaysFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	restDownTestCases := []struct {
		name                    string
		mode                    SubscriptionMode
		minCallFirstMsg         int
		minCallSecondMsg        int
		maxCallSecondMsg        int
		waitUntilCheckCallCount time.Duration
	}{
		{
			name:                    "Strict-In-Order Subscriber always fails, never calls second message",
			mode:                    SingleSubscriberStrictMessageOrder,
			minCallFirstMsg:         2,
			minCallSecondMsg:        0,
			maxCallSecondMsg:        0,
			waitUntilCheckCallCount: defaultNakDelay * 2,
		},
		{
			name:                    "MultipleSubscribersAllowed always fails, tries second message",
			mode:                    MultipleSubscribersAllowed,
			minCallFirstMsg:         2,
			minCallSecondMsg:        2,
			maxCallSecondMsg:        3,
			waitUntilCheckCallCount: defaultNakDelay * 2,
		},
	}
	subject := integrationTestStreamName + ".subscriberAlwaysFails"
	for _, test := range restDownTestCases {
		t.Run(test.name, func(t *testing.T) {
			conn := makeIntegrationTestConn(t)
			publishStringMessages(t, conn, subject, []string{"hello", "world"})
			sub := createSubscriber(t, conn, "TestSubscriberAlwaysFails", subject, test.mode)

			callCountHello, callCountWorld := 0, 0

			handler := func(msg Msg) error {
				switch string(msg.Data) {
				case "hello":
					callCountHello++
				case "world":
					callCountWorld++
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
		})
	}
}

type subscriberConfig struct {
	mode              SubscriptionMode
	alwaysFail        bool
	minSuccessfulMsgs int
	minFailedMsgs     int
}

type subscriptionState struct {
	subscriber     *Subscriber
	SuccessfulMsgs int
	FailedMsgs     int
}

func TestSubscriberMultiple(t *testing.T) {
	type args struct {
		subscribers             []subscriberConfig
		publishMessages         int
		waitUntilCheckCallCount time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "One Subscriber down, one up - messages will be handled by up Subscriber",
			args: args{
				subscribers: []subscriberConfig{
					{
						mode:              MultipleSubscribersAllowed,
						alwaysFail:        true,
						minSuccessfulMsgs: 0,
						minFailedMsgs:     1,
					},
					{
						mode:              MultipleSubscribersAllowed,
						alwaysFail:        false,
						minSuccessfulMsgs: 6,
						minFailedMsgs:     0,
					},
				},
				publishMessages:         6,
				waitUntilCheckCallCount: defaultNakDelay * 6,
			},
			wantErr: false,
		},
		{
			name: "Both Subscriber down, constantly retrieving naked messages",
			args: args{
				subscribers: []subscriberConfig{
					{
						mode:              MultipleSubscribersAllowed,
						alwaysFail:        false,
						minSuccessfulMsgs: 3,
						minFailedMsgs:     0,
					},
					{
						mode:              MultipleSubscribersAllowed,
						alwaysFail:        false,
						minSuccessfulMsgs: 3,
						minFailedMsgs:     0,
					},
				},
				publishMessages:         6,
				waitUntilCheckCallCount: defaultNakDelay,
			},
			wantErr: true,
		},
	}
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	subject := integrationTestStreamName + ".multipleSubscriber"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s []*subscriptionState

			conn := makeIntegrationTestConn(t)

			for idx, subConfig := range tt.args.subscribers {
				sub := createSubscriber(t, conn, "TestSubscriberAlwaysFails", subject, subConfig.mode)

				subState := subscriptionState{subscriber: sub}
				s = append(s, &subState)

				handler := makeHandlerSubscriber(t, subConfig.alwaysFail, &subState, idx)

				if err := s[idx].subscriber.Subscribe(handler); err != nil && !tt.wantErr {
					t.Error(err)
				}
			}

			publishManyMessages(t, conn, subject, tt.args.publishMessages)

			time.Sleep(tt.args.waitUntilCheckCallCount)

			for idx, subState := range s {
				if subState.FailedMsgs < tt.args.subscribers[idx].minFailedMsgs {
					t.Errorf("Subscriber %d: Too less messages failed %d < %d", idx, subState.FailedMsgs, tt.args.subscribers[idx].minFailedMsgs)
				}
				if subState.SuccessfulMsgs < tt.args.subscribers[idx].minSuccessfulMsgs {
					t.Errorf("Subscriber %d: Too less messages were successful %d < %d", idx, subState.SuccessfulMsgs, tt.args.subscribers[idx].minSuccessfulMsgs)
				}
			}
		})
	}
}

func makeHandlerSubscriber(t *testing.T, alwaysFail bool, s *subscriptionState, idx int) func(msg Msg) error {
	handler := func(msg Msg) error {
		if alwaysFail {
			s.FailedMsgs++
			t.Logf("Subscriber %v: Failed msg handeling, failesp: %v", idx, s.FailedMsgs)
			return fmt.Errorf("msg handleing failed")
		}
		s.SuccessfulMsgs++
		t.Logf("Subscriber %v: Successful msg handeling, successes: %v", idx, s.SuccessfulMsgs)
		return nil
	}
	return handler
}
