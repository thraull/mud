package nodep2p

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// TODO: rename this to be more specific
type ProtoStream struct {
	RW *bufio.ReadWriter
}

func (stream *ProtoStream) Read() ([]byte, error) {
	// Get the message size
	sizeBytes := make([]byte, 2)
	_, err := io.ReadFull(stream.RW, sizeBytes)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint16(sizeBytes)
	// Get the message
	data := make([]byte, size)
	_, err = io.ReadFull(stream.RW, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (stream *ProtoStream) Write(data []byte) error {
	size := len(data)
	if size > 65536 {
		return errors.New("data is over 64 KB limit")
	}
	// Encode data size in bytes
	sizeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(sizeBytes, uint16(size))
	// Write data size and data to buffer
	_, err := stream.RW.Write(append(sizeBytes, data...))
	if err != nil {
		return err
	}
	return nil
}
