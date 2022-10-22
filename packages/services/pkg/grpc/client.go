package grpc

import (
	"latticexyz/mud/packages/services/pkg/relay"
	"latticexyz/mud/packages/services/pkg/utils"
	pb_relay "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/avast/retry-go"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func getGrpcConnection(addr string, logger *zap.Logger) *grpc.ClientConn {
	var conn *grpc.ClientConn
	var retrying bool = false

	err := retry.Do(
		func() error {
			var err error
			// TODO: Set dial options [?]
			conn, err = grpc.Dial(addr)
			utils.LogErrorWhileRetrying("failed to get grpc connection", err, &retrying, logger)
			return err
		},
		utils.ServiceDelayType,
		utils.ServiceRetryAttempts,
		utils.ServiceRetryDelay,
	)

	if err != nil {
		logger.Fatal("failed while retrying to get grpc connection", zap.Error(err))
	}
	return conn
}

func NewP2PRelayClientDirect(config *relay.P2PRelayServerConfig, ethClient *ethclient.Client, logger *zap.Logger) *p2PRelayClientDirect {
	server := createP2PRelayServer(logger, ethClient, config)
	return &p2PRelayClientDirect{server: server}
}

func NewP2PRelayClientRemote(addr string, logger *zap.Logger) *p2PRelayClientRemote {
	conn := getGrpcConnection(addr, logger)
	client := pb_relay.NewP2PRelayServiceClient(conn)
	return &p2PRelayClientRemote{client: client}
}
