package main

import (
	"flag"

	"latticexyz/mud/packages/services/pkg/eth"
	"latticexyz/mud/packages/services/pkg/grpc"
	"latticexyz/mud/packages/services/pkg/logger"
	"latticexyz/mud/packages/services/pkg/nodep2p"
	"latticexyz/mud/packages/services/pkg/relay"
	"latticexyz/mud/packages/services/pkg/relayp2p"

	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	wsUrl                  = flag.String("ws-url", "ws://localhost:8545", "Websocket Url")
	port                   = flag.Int("port", 50071, "gRPC Server Port")
	idleTimeoutTime        = flag.Int("idle-timeout-time", 30, "Time in seconds after which a client connection times out. Defaults to 30s")
	idleDisconnectIterval  = flag.Int("idle-disconnect-interval", 60, "Time in seconds for how oftern to disconnect idle clients. Defaults to 60s")
	messsageDriftTime      = flag.Int("message-drift-time", 5, "Time in seconds that is acceptable as drift before message is not relayed. Defaults to 5s")
	minAccountBalance      = flag.Uint64("min-account-balance", 1000000000000000, "Minimum balance in wei for an account to get its messages relayed. Defaults to 0.001 ETH")
	verifyMessageSignature = flag.Bool("verify-msg-sig", false, "Whether to service-side verify the signature on each relayed message. Defaults to false.")
	verifyAccountBalance   = flag.Bool("verify-account-balance", false, "Whether to service-side verify that the account has sufficient balance when relaying message. Defaults to false.")
	messageRateLimit       = flag.Int("msg-rate-limit", 10, "Rate limit for messages per second that a single client can push to be relayed. Defaults to 10")
	metricsPort            = flag.Int("metrics-port", 6060, "Prometheus metrics http handler port. Defaults to port 6060")
	useP2P                 = flag.Bool("use-p2p", false, "Whether to connect to the relay p2p network. Defaults to false.")
	verifyP2PMessages      = flag.Bool("verify-p2p-msg", true, "Whether to verify messages received from the p2p node. Defaults to true.")
	p2pPort                = flag.Int("p2p-port", 35071, "P2P Node Port")
	p2pSeed                = flag.Int64("p2p-seed", 0, "P2P Node Private Key Seed")
	externalP2PNode        = flag.Bool("external-p2p-node", false, "Whether to connect to an external p2p node instead of running one internally. Defaults to false.")
	p2pAddress             = flag.String("p2p-address", "http://localhost:35000", "Address of the relay p2p server to connect to. Defaults to false.")
)

func main() {
	// Parse command line flags.
	flag.Parse()

	// Setup logging.
	logger.InitLogger()
	logger := logger.GetLogger()
	defer logger.Sync()

	// Build a config.
	config := &relay.RelayServerConfig{
		IdleTimeoutTime:        *idleTimeoutTime,
		IdleDisconnectIterval:  *idleDisconnectIterval,
		MessageDriftTime:       *messsageDriftTime,
		MinAccountBalance:      *minAccountBalance,
		VerifyMessageSignature: *verifyMessageSignature,
		VerifyAccountBalance:   *verifyAccountBalance,
		MessageRateLimit:       *messageRateLimit,
		UseP2P:                 *useP2P,
		VerifyP2PMessages:      *verifyP2PMessages,
	}

	var ethClient *ethclient.Client
	if *verifyAccountBalance {
		// Get an instance of ethereum client.
		ethClient = eth.GetEthereumClient(*wsUrl, logger)
	}

	// Get an instance of p2p client.
	var p2pClient pb.P2PRelayServiceClient
	if *useP2P {
		if *externalP2PNode {
			// Connect to an external p2p server.
			p2pClient = grpc.NewP2PRelayClientRemote(*p2pAddress, logger)
		} else {
			// Run and connect to an internal p2p relay node
			p2pNode := nodep2p.NewP2PNode(*p2pPort, *p2pSeed)
			p2pConfig := &relayp2p.P2PRelayServerConfig{
				MessageDriftTime:       *messsageDriftTime,
				MinAccountBalance:      *minAccountBalance,
				VerifyMessageSignature: *verifyMessageSignature,
				VerifyAccountBalance:   *verifyAccountBalance,
			}
			p2pClient = grpc.NewP2PRelayClientDirect(p2pConfig, ethClient, p2pNode, logger)
		}
	}

	// Start gRPC server and the relayer.
	grpc.StartRelayServer(*port, *metricsPort, ethClient, p2pClient, config, logger)
}
