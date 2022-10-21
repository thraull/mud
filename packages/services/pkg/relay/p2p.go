package relay

import (
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"go.uber.org/zap"
)

type P2PConn interface {
	// sendMany, receiveMany
	Send(*pb.PushRequest) error
	Receive() (*pb.PushRequest, error)
	IsRemote() bool
}

// MockP2PConn mocks a connection to a p2p node.
// Messages sent to it are looped as if they had been received.
type MockP2PConn struct {
	Logger *zap.Logger
	Loop   chan *pb.PushRequest
}

func (conn *MockP2PConn) Send(message *pb.PushRequest) error {
	conn.Logger.Info("sent p2p message")
	conn.Loop <- message
	return nil
}

func (conn *MockP2PConn) Receive() (*pb.PushRequest, error) {
	request := <-conn.Loop
	conn.Logger.Info("received p2p message")
	return request, nil
}

func (conn *MockP2PConn) IsRemote() bool {
	return false
}

func NewMockP2PConn(logger *zap.Logger) *MockP2PConn {
	return &MockP2PConn{Logger: logger, Loop: make(chan *pb.PushRequest)}
}
