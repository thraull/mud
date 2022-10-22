package grpc

import (
	"io"
	"latticexyz/mud/packages/services/pkg/relay"
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

type p2PRelayServer struct {
	pb.UnimplementedP2PRelayServiceServer
	config *relay.P2PRelayServerConfig
	logger *zap.Logger
	loop   chan *pb.PushRequest
}

func (server *p2PRelayServer) Init() {}

func (server *p2PRelayServer) OpenStream(_ *emptypb.Empty, stream pb.P2PRelayService_OpenStreamServer) error {
	for request := range server.loop {
		stream.Send(request)
		server.logger.Info("sent p2p request")
	}
	return nil
}

func (server *p2PRelayServer) PushStream(stream pb.P2PRelayService_PushStreamServer) error {
	// Continuously receive message relay requests, handle to relay, and respond with confirmations.
	for {
		// Receive request message from input stream.
		request, err := stream.Recv()

		if err == io.EOF {
			m := new(emptypb.Empty)
			stream.SendAndClose(m)
			return nil
		}
		if err != nil {
			return err
		}

		server.loop <- request
		server.logger.Info("received p2p request")
	}
}
