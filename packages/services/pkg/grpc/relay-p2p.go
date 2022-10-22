package grpc

import (
	"io"
	"latticexyz/mud/packages/services/pkg/relay"
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

// THIS IS A MOCK
// This code attempts to mimic the behavior of the unimplemented p2p system.
// The mock p2p server streams back the messages it receives from the client.

type p2PRelayServer struct {
	pb.UnimplementedP2PRelayServiceServer

	ethClient *ethclient.Client
	config    *relay.P2PRelayServerConfig
	logger    *zap.Logger

	loop chan *pb.PushRequest
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
	// Continuously receive message relay requests and handle them.
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
