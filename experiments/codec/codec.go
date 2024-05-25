package codec

import (
	"google.golang.org/grpc/encoding"
)

var DidInitRun bool = false

type TestCodec struct {
}

func init() {
	DidInitRun = true
	encoding.RegisterCodec(&TestCodec{})
}

// Marshal returns the wire format of v.
func (t *TestCodec) Marshal(v any) ([]byte, error) {
	message := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	return message, nil
}

// Unmarshal parses the wire format into v.
func (t *TestCodec) Unmarshal(data []byte, v any) error {
	return nil
}

// Name returns the name of the Codec implementation. The returned string
// will be used as part of content type in transmission.  The result must be
// static; the result cannot change between calls.
func (t *TestCodec) Name() string {
	return "test"
}
