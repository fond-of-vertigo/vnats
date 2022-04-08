package vnats

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nats-io/nats.go"
	"reflect"
	"testing"
)

func Test_msgHandler_handle(t *testing.T) {
	type testStruct struct {
		Test string
	}

	type testData struct {
		name         string
		handler      MsgHandler
		encoding     MsgEncoding
		msg          nats.Msg
		decodedValue interface{}
		recvSubject  string
		recvReply    string
		wantValue    interface{}
		wantErr      bool
		initFunc     func(tt *testData)
	}

	tests := []testData{{
		name: "Test handle raw msg",
		msg: nats.Msg{
			Data: []byte(`{"Test":"string content"}`),
		},
		initFunc: func(tt *testData) {
			tt.wantValue = tt.msg
			tt.handler = func(msg *nats.Msg) error {
				tt.decodedValue = *msg
				return nil
			}
		},
	}, {
		name: "Test handle JSON",
		msg: nats.Msg{
			Data: []byte(`{"Test":"string content"}`),
		},
		wantValue: testStruct{Test: "string content"},
		initFunc: func(tt *testData) {
			tt.handler = func(ts *testStruct) error {
				tt.decodedValue = *ts
				return nil
			}
		},
	}, {
		name:     "Test handle raw byte array",
		encoding: EncRaw,
		msg: nats.Msg{
			Data: []byte(`content`),
		},
		wantValue: []byte(`content`),
		initFunc: func(tt *testData) {
			tt.handler = func(data []byte) error {
				tt.decodedValue = data
				return nil
			}
		},
	}, {
		name:     "Test handle raw string",
		encoding: EncRaw,
		msg: nats.Msg{
			Data: []byte("my test string"),
		},
		wantValue: "my test string",
		initFunc: func(tt *testData) {
			tt.handler = func(text string) error {
				tt.decodedValue = text
				return nil
			}
		},
	}, {
		name: "Test subject arg",
		msg: nats.Msg{
			Subject: "mysubject",
			Data:    []byte(`{"Test":"string content"}`),
		},
		wantValue: testStruct{Test: "string content"},
		initFunc: func(tt *testData) {
			tt.handler = func(subject string, ts *testStruct) error {
				tt.recvSubject = subject
				tt.decodedValue = *ts
				return nil
			}
		},
	}, {
		name: "Test subject and reply args",
		msg: nats.Msg{
			Subject: "mysubject",
			Reply:   "myreply",
			Data:    []byte(`{"Test":"string content"}`),
		},
		wantValue: testStruct{Test: "string content"},
		initFunc: func(tt *testData) {
			tt.handler = func(subject, reply string, ts *testStruct) error {
				tt.recvSubject = subject
				tt.recvReply = reply
				tt.decodedValue = *ts
				return nil
			}
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.initFunc != nil {
				tt.initFunc(&tt)
			}

			mh, err := makeMsgHandler(tt.encoding, tt.handler)
			if err != nil {
				t.Fatalf("failed to create msgHandler: %s", err)
			}

			err = mh.handle(&tt.msg)
			if err != nil {
				if tt.wantErr {
					return
				}

				t.Errorf("decodePayload() error = %v, wantErr %v", err, tt.wantErr)
			}

			diff := cmp.Diff(tt.decodedValue, tt.wantValue, cmpopts.IgnoreUnexported(nats.Msg{}))
			if diff != "" {
				t.Errorf("diff in decoded value:\n%s", diff)
			}

			if tt.recvSubject != tt.msg.Subject {
				t.Errorf("subject does not match: got \"%s\" expected \"%s\"", tt.recvSubject, tt.msg.Subject)
			}

			if tt.recvReply != tt.msg.Reply {
				t.Errorf("reply does not match: got \"%s\" expected \"%s\"", tt.recvReply, tt.msg.Reply)
			}
		})
	}
}

func Test_makeMsgHandler(t *testing.T) {
	type testStruct struct {
		Test string
	}
	
	type args struct {
		handler  MsgHandler
		encoding MsgEncoding
	}

	tests := []struct {
		name    string
		args    args
		wantMh  *msgHandler
		wantErr bool
	}{{
		name: "Test NOP byte array handler",
		args: args{
			handler:  func(data []byte) error { return nil },
			encoding: EncRaw,
		},
		wantMh: &msgHandler{
			encoding:     EncRaw,
			funcArgs:     make([]reflect.Value, 1),
			valueArgType: reflect.TypeOf([]byte{}),
		},
	}, {
		name: "Test NOP EncJSON handler",
		args: args{
			handler:  func(ts *testStruct) error { return nil },
			encoding: EncJSON,
		},
		wantMh: &msgHandler{
			encoding:     EncJSON,
			funcArgs:     make([]reflect.Value, 1),
			valueArgType: reflect.TypeOf(&testStruct{}),
		},
	}, {
		name: "Test subject handler",
		args: args{
			handler:  func(subject string, ts *testStruct) error { return nil },
			encoding: EncJSON,
		},
		wantMh: &msgHandler{
			encoding:     EncJSON,
			funcArgs:     make([]reflect.Value, 2),
			valueArgType: reflect.TypeOf(&testStruct{}),
		},
	}, {
		name: "Test subject reply handler",
		args: args{
			handler:  func(subject, reply string, ts *testStruct) error { return nil },
			encoding: EncJSON,
		},
		wantMh: &msgHandler{
			encoding:     EncJSON,
			funcArgs:     make([]reflect.Value, 3),
			valueArgType: reflect.TypeOf(&testStruct{}),
		},
	}, {
		name: "Test wantsRawMsg handler",
		args: args{
			handler: func(msg *nats.Msg) error { return nil },
		},
		wantMh: &msgHandler{
			funcArgs:     make([]reflect.Value, 1),
			valueArgType: reflect.TypeOf(&testStruct{}),
			wantsRawMsg:  true,
		},
	}, {
		name: "Test no return valuer",
		args: args{
			handler: func(ts *testStruct) {},
		},
		wantErr: true,
	}, {
		name: "Test invalid return value type",
		args: args{
			handler: func(ts *testStruct) int { return 0 },
		},
		wantErr: true,
	}, {
		name: "Test invalid argument types",
		args: args{
			handler: func(n, m int, ts *testStruct) error { return nil },
		},
		wantErr: true,
	}, {
		name: "Test handler is no function",
		args: args{
			handler: "",
		},
		wantErr: true,
	}, {
		name: "Test no arguments",
		args: args{
			handler: func() error { return nil },
		},
		wantErr: true,
	}, {
		name: "Test too many arguments",
		args: args{
			handler: func(subject, reply string, data *testStruct, extraArg string) error { return nil },
		},
		wantErr: true,
	},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantMh != nil {
				tt.wantMh.funcValue = reflect.ValueOf(tt.args.handler)
			}

			gotMh, err := makeMsgHandler(tt.args.encoding, tt.args.handler)
			if err != nil {
				if tt.wantErr {
					return
				}

				t.Errorf("makeMsgHandler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotMh.encoding != tt.wantMh.encoding {
				t.Errorf("makeMsgHandler(): gotMh.encoding = %v, wantMh.encoding = %v", gotMh, tt.wantMh)
			}
		})
	}
}
