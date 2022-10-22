package grpc

import (
	"latticexyz/mud/packages/services/pkg/relay"
	pb_relay "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewP2PRelayClientDirect(config *relay.P2PRelayServerConfig, ethClient *ethclient.Client, logger *zap.Logger) *p2PRelayClientDirect {
	server := createP2PRelayServer(logger, ethClient, config)
	return &p2PRelayClientDirect{server: server}
}

func NewP2PRelayClientRemote(addr string, logger *zap.Logger) *p2PRelayClientRemote {
	// TODO: Set dial options
	conn, err := grpc.Dial(addr)
	if err != nil {
		logger.Info("error dialing remote p2p server", zap.Error(err))
		return nil
	}
	client := pb_relay.NewP2PRelayServiceClient(conn)
	return &p2PRelayClientRemote{client: client}
}
