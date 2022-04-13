package vnats

import (
	"errors"
	"github.com/google/go-cmp/cmp"
	"reflect"
	"testing"
)

func Test_encodePayload(t *testing.T) {
	type testStruct struct {
		Test string
	}

	type testData struct {
		name           string
		encoding       MsgEncoding
		stringValue    string
		byteArrayValue []byte
		data           interface{}
		want           []byte
		wantErr        bool
		initFunc       func(td *testData)
	}

	tests := []testData{{
		name: "Test JSON encoding",
		data: testStruct{Test: "Hallo"},
		want: []byte(`{"Test":"Hallo"}`),
	}, {
		name:     "Test invalid encoding",
		encoding: MsgEncoding(2),
		wantErr:  true,
	}, {
		name:     "Test raw byte array",
		encoding: EncRaw,
		data:     []byte("Test"),
		want:     []byte("Test"),
	}, {
		name:           "Test raw byte array pointer",
		encoding:       EncRaw,
		byteArrayValue: []byte("TestPtr"),
		want:           []byte("TestPtr"),
		initFunc: func(td *testData) {
			td.data = &td.byteArrayValue
		},
	}, {
		name:     "Test raw string",
		encoding: EncRaw,
		data:     "Test",
		want:     []byte("Test"),
	}, {
		name:        "Test raw string pointer",
		encoding:    EncRaw,
		stringValue: "TestStrPtr",
		want:        []byte("TestStrPtr"),
		initFunc: func(td *testData) {
			td.data = &td.stringValue
		},
	}, {
		name:     "Test raw unsupported value",
		encoding: EncRaw,
		data:     testStruct{Test: "invalid"},
		wantErr:  true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.initFunc != nil {
				tt.initFunc(&tt)
			}

			got, err := encodePayload(tt.encoding, tt.data)
			if err != nil {
				if tt.wantErr {
					if !errors.Is(err, ErrEncodePayload) {
						t.Errorf("error chain does not contain ErrDecodePayload")
					}
					return
				}

				t.Errorf("encodePayload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			diff := cmp.Diff(string(got), string(tt.want))
			if diff != "" {
				t.Errorf("encodePayload failed: \n%s", diff)
			}
		})
	}
}

func Test_decodePayload(t *testing.T) {
	type testStruct struct {
		Test string
	}

	type testData struct {
		name         string
		data         []byte
		encoding     MsgEncoding
		byteData     []byte
		stringData   string
		decodedValue interface{}
		wantValue    interface{}
		wantErr      bool
		initFunc     func(tt *testData)
	}

	tests := []testData{{
		name:         "Test decode JSON",
		data:         []byte(`{"Test":"string content"}`),
		decodedValue: &testStruct{},
		wantValue:    &testStruct{Test: "string content"},
	}, {
		name:         "Test decode JSON error",
		data:         []byte(`{"Test":false}`),
		decodedValue: &testStruct{},
		wantErr:      true,
	}, {
		name:      "Test decode raw byte array",
		data:      []byte("Test data"),
		encoding:  EncRaw,
		byteData:  []byte("Test data"),
		wantValue: []byte("Test data"),
		initFunc: func(tt *testData) {
			tt.decodedValue = &tt.byteData
		},
	}, {
		name:       "Test decode raw string",
		data:       []byte("Test string"),
		encoding:   EncRaw,
		stringData: "",
		wantValue:  "Test string",
		initFunc: func(tt *testData) {
			tt.decodedValue = &tt.stringData
		},
	}, {
		name:     "Test invalid raw value",
		encoding: EncRaw,
		wantErr:  true,
	}, {
		name:     "Test invalid encoding type",
		encoding: MsgEncoding(2),
		wantErr:  true,
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.initFunc != nil {
				tt.initFunc(&tt)
			}

			err := decodePayload(tt.encoding, tt.data, tt.decodedValue)
			if err != nil {
				if tt.wantErr {
					if !errors.Is(err, ErrDecodePayload) {
						t.Errorf("error chain does not contain ErrDecodePayload")
					}
					return
				}

				t.Errorf("decodePayload() error = %v, wantErr %v", err, tt.wantErr)
			}

			v := tt.decodedValue
			if reflect.ValueOf(tt.decodedValue).Kind() == reflect.Ptr && reflect.ValueOf(tt.wantValue).Kind() != reflect.Ptr {
				v = reflect.ValueOf(tt.decodedValue).Elem().Interface()
			}
			diff := cmp.Diff(v, tt.wantValue)
			if diff != "" {
				t.Errorf("diff in decoded value:\n%s", diff)
			}
		})
	}
}
