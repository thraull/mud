package nodep2p

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/libp2p/go-libp2p/core/network"
)

// TODO: rename this to be more specific

func NewReader(stream network.Stream) *ProtoStreamReader {
	return &ProtoStreamReader{Reader: bufio.NewReader(stream)}
}

func NewWriter(stream network.Stream) *ProtoStreamWriter {
	// TODO: issue: stream blocks when writes are buffered
	return &ProtoStreamWriter{Writer: stream}
}

type ProtoStreamReader struct {
	Reader io.Reader
}

func (stream *ProtoStreamReader) Read() ([]byte, error) {
	// Get the message size
	sizeBytes := make([]byte, 2)
	_, err := io.ReadFull(stream.Reader, sizeBytes)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint16(sizeBytes)
	// Get the message
	data := make([]byte, size)
	_, err = io.ReadFull(stream.Reader, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type ProtoStreamWriter struct {
	Writer io.Writer
}

func (stream *ProtoStreamWriter) Write(data []byte) error {
	size := len(data)
	if size > math.MaxUint16 {
		return errors.New("data is over 64 KB limit")
	}
	// Encode data size in bytes
	sizeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(sizeBytes, uint16(size))
	// Write data size and data to buffer
	_, err := stream.Writer.Write(append(sizeBytes, data...))
	if err != nil {
		return err
	}
	return nil
}
