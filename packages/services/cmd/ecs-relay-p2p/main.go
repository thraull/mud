package main

import (
	"flag"

	"latticexyz/mud/packages/services/pkg/eth"
	"latticexyz/mud/packages/services/pkg/grpc"
	"latticexyz/mud/packages/services/pkg/logger"
	"latticexyz/mud/packages/services/pkg/relay"
)

var (
	wsUrl                  = flag.String("ws-url", "ws://localhost:8545", "Websocket Url")
	port                   = flag.Int("port", 50071, "gRPC Server Port")
	messageDriftTime       = flag.Int("message-drift-time", 5, "Time in seconds that is acceptable as drift before message is not relayed. Defaults to 5s")
	minAccountBalance      = flag.Uint64("min-account-balance", 1000000000000000, "Minimum balance in wei for an account to get its messages relayed. Defaults to 0.001 ETH")
	verifyMessageSignature = flag.Bool("verify-msg-sig", false, "Whether to service-side verify the signature on each relayed message. Defaults to false.")
	verifyAccountBalance   = flag.Bool("verify-account-balance", false, "Whether to service-side verify that the account has sufficient balance when relaying message. Defaults to false.")
	metricsPort            = flag.Int("metrics-port", 6060, "Prometheus metrics http handler port. Defaults to port 6060")
)

func main() {
	// Parse command line flags.
	flag.Parse()

	// Setup logging.
	logger.InitLogger()
	logger := logger.GetLogger()
	defer logger.Sync()

	config := &relay.P2PRelayServerConfig{
		MessageDriftTime:       *messageDriftTime,
		MinAccountBalance:      *minAccountBalance,
		VerifyMessageSignature: *verifyMessageSignature,
		VerifyAccountBalance:   *verifyAccountBalance,
	}

	// Get an instance of ethereum client.
	ethClient := eth.GetEthereumClient(*wsUrl, logger)

	// Start gRPC server and the p2p node (p2p not yet implemented).
	grpc.StartP2PRelayServer(*port, *metricsPort, ethClient, config, logger)
}
