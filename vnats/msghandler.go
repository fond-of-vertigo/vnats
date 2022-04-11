package vnats

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"reflect"
)

// MsgHandler is a generic interface that supports multiple subscriber
// callback function signatures. It also handles message decoding depending on
// the encoding setting of the subscriber.
// The design is inspired by the (now deprecated) nats.EncodedConn.
//
// The following callback signatures are supported:
//
//	handler := func(o *YourStructType)
//	handler := func(subject string, o *YourStructType)
//	handler := func(subject, reply string, o *YourStructType)
//
// You can also get the raw nats Msg, but this usage is discouraged in production
// code, as you leave protected vnats paths then. But it may be useful in some
// edge cases. The callback signature looks like this:
//
//	handler := func(msg *nats.Msg)
//
type MsgHandler interface{}

type msgHandler struct {
	encoding     MsgEncoding
	funcValue    reflect.Value
	funcArgs     []reflect.Value
	valueArgType reflect.Type
	wantsRawMsg  bool
}

func makeMsgHandler(encoding MsgEncoding, handler MsgHandler) (mh *msgHandler, err error) {
	mh = &msgHandler{
		encoding: encoding,
	}
	funcType := reflect.TypeOf(handler)
	if funcType.Kind() != reflect.Func {
		return nil, fmt.Errorf("type of MsgHandler must be a <func>, but is <%s>", funcType)
	}

	numArgs := funcType.NumIn()
	if numArgs == 0 || numArgs > 3 {
		return nil, fmt.Errorf("MsgHandler func must have 1, 2 or 3 arguments, but is has %d arguments", numArgs)
	}

	mh.valueArgType = funcType.In(numArgs - 1)
	for i := 0; i < numArgs-1; i++ {
		argType := funcType.In(i)
		if argType.Kind() != reflect.String {
			return nil, fmt.Errorf("MsgHandler argument %d must be of type <string>, it is <%s>", i+1, argType)
		}
	}

	numReturn := funcType.NumOut()
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if numReturn != 1 || !funcType.Out(0).Implements(errorInterface) {
		return nil, fmt.Errorf("MsgHandler func must have one return value of type <error>")
	}

	mh.funcValue = reflect.ValueOf(handler)
	mh.funcArgs = make([]reflect.Value, numArgs)
	mh.wantsRawMsg = numArgs == 1 && mh.valueArgType == reflect.TypeOf(&nats.Msg{})

	return mh, nil
}

func (mh *msgHandler) handle(msg *nats.Msg) (err error) {
	if mh.wantsRawMsg {
		mh.funcArgs[0] = reflect.ValueOf(msg)
	} else {
		var oPtr reflect.Value
		if mh.valueArgType.Kind() == reflect.Ptr {
			oPtr = reflect.New(mh.valueArgType.Elem())
		} else {
			oPtr = reflect.New(mh.valueArgType)
		}

		err = decodePayload(mh.encoding, msg.Data, oPtr.Interface())
		if err != nil {
			return err
		}

		if mh.valueArgType.Kind() != reflect.Ptr {
			oPtr = reflect.Indirect(oPtr)
		}

		switch len(mh.funcArgs) {
		case 1:
			mh.funcArgs[0] = oPtr
		case 2:
			mh.funcArgs[0] = reflect.ValueOf(msg.Subject)
			mh.funcArgs[1] = oPtr
		case 3:
			mh.funcArgs[0] = reflect.ValueOf(msg.Subject)
			mh.funcArgs[1] = reflect.ValueOf(msg.Reply)
			mh.funcArgs[2] = oPtr
		}
	}

	outValue := mh.funcValue.Call(mh.funcArgs)[0]
	if !outValue.IsNil() {
		return outValue.Interface().(error)
	}

	return nil
}
