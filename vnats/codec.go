package vnats

import (
	"encoding/json"
	"errors"
	"fmt"
)

// MsgEncoding specifies if and how the payload should be encoded (raw or JSON marshalling).
type MsgEncoding int

const (
	// EncJSON means that the vnats lib will marshal and unmarshal messages to JSON for you.
	// This is the default encoding.
	EncJSON MsgEncoding = iota

	// EncRaw means that the message data is not encoded or decoded.
	// You must use byte array or string message content.
	EncRaw
)

var (
	// ErrEncodePayload is in the error chain if encoding the payload failed (JSON marshalling).
	// To check for this type of error use:
	//  errors.Is(err, ErrEncodePayload)
	ErrEncodePayload = errors.New("invalid payload, encoding failed")

	// ErrDecodePayload is in the error chain if decoding the payload failed (JSON unmarshalling).
	// To check for this type of error use:
	//  errors.Is(err, ErrDecodePayload)
	ErrDecodePayload = errors.New("invalid payload, decoding failed")
)

func encodePayload(encoding MsgEncoding, data interface{}) ([]byte, error) {
	switch encoding {
	case EncRaw:
		switch arg := data.(type) {
		case []byte:
			return arg, nil
		case *[]byte:
			return *arg, nil
		case string:
			return []byte(arg), nil
		case *string:
			return []byte(*arg), nil
		default:
			return nil, fmt.Errorf("%w: expected <[]byte> or <string>, got <%s>", ErrEncodePayload, arg)
		}
	case EncJSON:
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("%w: json.Marshal error: %s", ErrEncodePayload, err.Error())
		}
		return jsonData, nil
	default:
		return nil, fmt.Errorf("%w: unsupported encoding type <%d>", ErrEncodePayload, encoding)
	}
}

func decodePayload(encoding MsgEncoding, data []byte, vPtr interface{}) error {
	switch encoding {
	case EncRaw:
		switch arg := vPtr.(type) {
		case *[]byte:
			*arg = data
			return nil
		case *string:
			*arg = string(data)
			return nil
		default:
			return fmt.Errorf("%w: expected <[]byte> or <string>, got <%s>", ErrDecodePayload, arg)
		}
	case EncJSON:
		err := json.Unmarshal(data, vPtr)
		if err != nil {
			return fmt.Errorf("%w: json.Unmarshal error: %s", ErrDecodePayload, err.Error())
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported encoding type <%d>", ErrDecodePayload, encoding)
	}
}
