package vnats

import (
	"encoding/json"
	"github.com/fond-of/logging.go/logger"
	"reflect"
	"testing"
)

type testMessagePayload struct {
	message string
}

func Test_publisher_Publish(t *testing.T) {
	type args struct {
		data       interface{}
		streamName string
		subject    string
		msgId      string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Publish test message",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "MESSAGES",
				subject:    "MESSAGES.Important",
				msgId:      "msg-001",
			},
			wantErr: false,
		},
		{
			name: "Publish to subject not starting with streamName",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "MESSAGES",
				subject:    "Important",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty subject",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "MESSAGES",
				subject:    "",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "",
				subject:    "MESSAGES",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName & empty subject",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "",
				subject:    "",
				msgId:      "msg-001",
			},
			wantErr: true,
		},

		{
			name: "Publish to subject starting with .",

			args: args{
				data:       testMessagePayload{message: "hello world"},
				streamName: "MESSAGES",
				subject:    ".Messages.Important",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantDataToBytes, err := json.Marshal(tt.args.data)
			if err != nil {
				t.Error(err)
			}
			pub := &publisher{
				conn:       makeTestConnection(tt.args.streamName, 1, wantDataToBytes, tt.args.msgId, nil),
				log:        testLogger,
				streamName: tt.args.streamName,
			}
			err = pub.Publish(tt.args.subject, tt.args.data, tt.args.msgId)
			if (err != nil) != tt.wantErr {
				t.Errorf("Publish() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}

func Test_makePublisher(t *testing.T) {
	type args struct {
		conn       *connection
		streamName string
		logger     logger.Logger
	}
	natsTestBridge := makeTestNATSBridge("PRODUCTS", 1, nil, "test")
	connectionEmptySubscriptions := &connection{
		nats:          natsTestBridge,
		log:           testLogger,
		subscriptions: nil,
	}
	tests := []struct {
		name    string
		args    args
		want    *publisher
		wantErr bool
	}{
		{
			name: "Default publisher generation.",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS",
				logger:     testLogger,
			},
			want: &publisher{
				conn: connectionEmptySubscriptions,
				log:  testLogger,
			},
			wantErr: false,
		},
		{
			name: "No StreamName specified",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "",
				logger:     testLogger,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains *",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS*",
				logger:     testLogger,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains .",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS.",
				logger:     testLogger,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains >",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS>",
				logger:     testLogger,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makePublisher(tt.args.conn, tt.args.streamName, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("makePublisher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makePublisher() got = %v, want %v", got, tt.want)
			}
		})
	}
}
