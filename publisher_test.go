package vnats

import (
	"encoding/json"
	"reflect"
	"testing"
)

type testMessagePayload struct {
	Message string `json:"message"`
}

func Test_publisher_Publish(t *testing.T) {
	type args struct {
		data       []byte
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
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    "MESSAGES.Important",
				msgId:      "msg-001",
			},
			wantErr: false,
		},
		{
			name: "Publish to subject not starting with streamName",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    "Important",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty subject",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    "",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName",

			args: args{
				data:       []byte("test message"),
				streamName: "",
				subject:    "MESSAGES",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName & empty subject",

			args: args{
				data:       []byte("test message"),
				streamName: "",
				subject:    "",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to subject starting with .",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    ".MESSAGES.Important",
				msgId:      "msg-001",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantDataToBytes, marshalErr := json.Marshal(tt.args.data)
			pub := &publisher{
				conn:       makeTestConnection(tt.args.streamName, 1, wantDataToBytes, tt.args.msgId, nil),
				log:        testLogger,
				streamName: tt.args.streamName,
			}
			err := pub.Publish(&OutMsg{
				Subject: tt.args.subject,
				MsgID:   tt.args.msgId,
				Data:    tt.args.data,
			})
			if ((err != nil) != tt.wantErr) && ((marshalErr != nil) != tt.wantErr) {
				var foundErr error
				if err != nil {
					foundErr = err
				} else {
					foundErr = marshalErr
				}
				t.Errorf("Publish() error = %v, wantErr %v", foundErr, tt.wantErr)
				return
			}

		})
	}
}

func Test_makePublisher(t *testing.T) {
	type args struct {
		conn       *connection
		streamName string
	}

	natsTestBridge := makeTestNATSBridge("PRODUCTS", 1, nil, "test")
	connectionEmptySubscriptions := &connection{
		nats:        natsTestBridge,
		log:         testLogger,
		subscribers: nil,
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
			},
			want: &publisher{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS",
				log:        testLogger,
			},
			wantErr: false,
		},
		{
			name: "No StreamName specified",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains *",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS*",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains .",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS.",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "StreamName contains >",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS>",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makePublisher(tt.args.conn, &NewPublisherArgs{
				StreamName: tt.args.streamName,
			})
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
