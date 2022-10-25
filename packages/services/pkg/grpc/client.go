package grpc

import (
	"latticexyz/mud/packages/services/pkg/nodep2p"
	"latticexyz/mud/packages/services/pkg/relayp2p"
	"latticexyz/mud/packages/services/pkg/utils"
	pb_relay "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/avast/retry-go"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getGrpcConnection(addr string, logger *zap.Logger) *grpc.ClientConn {
	var conn *grpc.ClientConn
	var retrying bool = false

	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		// grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		// grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	err := retry.Do(
		func() error {
			var err error
			// TODO: Set dial options [?]
			conn, err = grpc.Dial(addr, dialOpts...)
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

func NewP2PRelayClientDirect(config *relayp2p.P2PRelayServerConfig, ethClient *ethclient.Client, p2pNode *nodep2p.Node, logger *zap.Logger) *p2PRelayClientDirect {
	server := createP2PRelayServer(logger, ethClient, p2pNode, config)
	return &p2PRelayClientDirect{server: server}
}

func NewP2PRelayClientRemote(addr string, logger *zap.Logger) *p2PRelayClientRemote {
	conn := getGrpcConnection(addr, logger)
	client := pb_relay.NewP2PRelayServiceClient(conn)
	return &p2PRelayClientRemote{client: client}
}
