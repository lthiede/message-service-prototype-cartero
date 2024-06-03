package readertobytereader

import (
	"net"
)

type ReaderByteReader struct {
	net.Conn
}

func (r *ReaderByteReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := r.Conn.Read(b)
	return b[0], err
}
