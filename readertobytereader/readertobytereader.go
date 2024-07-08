package readertobytereader

import "io"

type ReaderByteReader struct {
	io.Reader
}

func (r *ReaderByteReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := r.Reader.Read(b)
	return b[0], err
}
