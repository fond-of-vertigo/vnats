package vnats

import (
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
		msgID      string
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
				msgID:      "msg-001",
			},
			wantErr: false,
		},
		{
			name: "Publish to subject not starting with streamName",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    "Important",
				msgID:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty subject",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    "",
				msgID:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName",

			args: args{
				data:       []byte("test message"),
				streamName: "",
				subject:    "MESSAGES",
				msgID:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to empty streamName & empty subject",

			args: args{
				data:       []byte("test message"),
				streamName: "",
				subject:    "",
				msgID:      "msg-001",
			},
			wantErr: true,
		},
		{
			name: "Publish to subject starting with .",

			args: args{
				data:       []byte("test message"),
				streamName: "MESSAGES",
				subject:    ".MESSAGES.Important",
				msgID:      "msg-001",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := testLogger{t}
			pub := &Publisher{
				conn:       makeTestConnection(t, tt.args.streamName, 1, tt.args.data, tt.args.msgID, nil),
				log:        tf.Logf,
				streamName: tt.args.streamName,
			}
			err := pub.Publish(&Msg{
				Subject: tt.args.subject,
				MsgID:   tt.args.msgID,
				Data:    tt.args.data,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("Publisher.Publish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_makePublisher(t *testing.T) {
	type args struct {
		conn       *Connection
		streamName string
	}

	natsTestBridge := makeTestNATSBridge(t, "PRODUCTS", 1, nil, "test")
	tf := testLogger{t}
	connectionEmptySubscriptions := &Connection{
		nats:        natsTestBridge,
		log:         tf.Logf,
		subscribers: nil,
	}

	tests := []struct {
		name    string
		args    args
		want    *Publisher
		wantErr bool
	}{
		{
			name: "Default Publisher generation.",
			args: args{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS",
			},
			want: &Publisher{
				conn:       connectionEmptySubscriptions,
				streamName: "PRODUCTS",
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
			got, err := tt.args.conn.CreatePublisher(CreatePublisherArgs{
				StreamName: tt.args.streamName,
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("makePublisher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != nil && tt.want == nil {
				t.Errorf("makePublisher() got = %v, want %v", got, tt.want)
			}

			if tt.want != nil && got.streamName != tt.want.streamName {
				t.Errorf("makePublisher() got = %v, want %v", got.streamName, tt.want.streamName)
			}
		})
	}
}
