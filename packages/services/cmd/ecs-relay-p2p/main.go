package main

import (
	"flag"

	"latticexyz/mud/packages/services/pkg/eth"
	"latticexyz/mud/packages/services/pkg/grpc"
	"latticexyz/mud/packages/services/pkg/logger"
	"latticexyz/mud/packages/services/pkg/nodep2p"
	"latticexyz/mud/packages/services/pkg/relayp2p"

	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	wsUrl                  = flag.String("ws-url", "ws://localhost:8545", "Websocket Url")
	grpcPort               = flag.Int("grpc-port", 50071, "gRPC Server Port")
	messageDriftTime       = flag.Int("message-drift-time", 5, "Time in seconds that is acceptable as drift before message is not relayed. Defaults to 5s")
	minAccountBalance      = flag.Uint64("min-account-balance", 1000000000000000, "Minimum balance in wei for an account to get its messages relayed. Defaults to 0.001 ETH")
	verifyMessageSignature = flag.Bool("verify-msg-sig", false, "Whether to service-side verify the signature on each relayed message. Defaults to false.")
	verifyAccountBalance   = flag.Bool("verify-account-balance", false, "Whether to service-side verify that the account has sufficient balance when relaying message. Defaults to false.")
	metricsPort            = flag.Int("metrics-port", 6060, "Prometheus metrics http handler port. Defaults to port 6060")
	p2pPort                = flag.Int("p2p-port", 35071, "P2P Node Port")
	p2pSeed                = flag.Int64("p2p-seed", 0, "P2P Node Private Key Seed")
	// idleDisconnectInterval
)

func main() {
	// Parse command line flags.
	flag.Parse()

	// Setup logging.
	logger.InitLogger()
	logger := logger.GetLogger()
	defer logger.Sync()

	config := &relayp2p.P2PRelayServerConfig{
		MessageDriftTime:       *messageDriftTime,
		MinAccountBalance:      *minAccountBalance,
		VerifyMessageSignature: *verifyMessageSignature,
		VerifyAccountBalance:   *verifyAccountBalance,
	}

	// Get an instance of ethereum client.
	var ethClient *ethclient.Client
	if *verifyAccountBalance {
		// Get an instance of ethereum client.
		ethClient = eth.GetEthereumClient(*wsUrl, logger)
	}

	// Get an instance of p2p node
	nodep2p := nodep2p.NewP2PNode(*p2pPort, *p2pSeed)

	// Start gRPC server and the p2p node (unimplemented).
	grpc.StartP2PRelayServer(*grpcPort, *metricsPort, ethClient, nodep2p, config, logger)
}
