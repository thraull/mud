package grpc

import (
	"bufio"
	"fmt"
	"io"
	"latticexyz/mud/packages/services/pkg/eth"
	"latticexyz/mud/packages/services/pkg/nodep2p"
	"latticexyz/mud/packages/services/pkg/relayp2p"
	"latticexyz/mud/packages/services/pkg/utils"
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type p2PRelayServer struct {
	pb.UnimplementedP2PRelayServiceServer
	relayp2p.SignerRegistry
	relayp2p.ClientRegistry
	relayp2p.PeerRegistry

	ethClient     *ethclient.Client
	nodep2p       host.Host
	config        *relayp2p.P2PRelayServerConfig
	knownMessages *utils.KnownCache
	logger        *zap.Logger
}

func (server *p2PRelayServer) Init() {
	// TODO: do the math on cache size
	server.knownMessages = utils.NewKnownCache(1048576)
	// Set up the stream handler
	server.nodep2p.SetStreamHandler("/relay/0.1.0", server.P2PStreamHandler)
}

func (server *p2PRelayServer) OpenStream(_ *emptypb.Empty, stream pb.P2PRelayService_OpenStreamServer) error {
	id, err := utils.GenerateRandomIdentifier()
	if err != nil {
		return err
	}
	client := server.ClientRegistry.NewClient(id, server.config)
	relayedMessagesChannel := client.GetChannel()
	for {
		select {
		case <-stream.Context().Done():
			server.logger.Info("client closed stream")
			server.ClientRegistry.RemoveClient(client.GetId())
			return nil
		case relayedMessage := <-relayedMessagesChannel:
			if relayedMessage == nil {
				server.logger.Warn("relayed message is nil")
			} else {
				stream.Send(relayedMessage)
			}
		}
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

	}
}

func (server *p2PRelayServer) EncodeMessage(message *pb.Message) string {
	return fmt.Sprintf("(%d,%s,%s,%d)", message.Version, message.Id, crypto.Keccak256Hash(message.Data).Hex(), message.Timestamp)
}

func (server *p2PRelayServer) HashMessage(message *pb.Message) []byte {
	messagePacked := server.EncodeMessage(message)
	hash := accounts.TextHash([]byte(messagePacked))
	return hash
}

func (server *p2PRelayServer) VerifyMessageSignature(message *pb.Message, identity *pb.Identity) (bool, string, error) {
	// First encode the message.
	messagePacked := server.EncodeMessage(message)

	// Get the 'from' address, or if not specified, make an empty string placeholder, since the
	// verification will fail anyways but the caller may want to use the recovered address.
	var from string
	if identity == nil {
		from = ""
	} else {
		from = identity.Name
	}
	isVerified, recoveredAddress, err := utils.VerifySig(
		from,
		message.Signature,
		[]byte(messagePacked),
	)
	return isVerified, recoveredAddress, err
}

func (server *p2PRelayServer) VerifyMessage(message *pb.Message, identity *pb.Identity) error {
	if message == nil {
		return fmt.Errorf("message is not defined")
	}
	if identity == nil {
		return fmt.Errorf("identity is not defined")
	}
	if len(message.Signature) == 0 {
		return fmt.Errorf("signature is not defined")
	}

	// Verify that the message is OK to relay if config flag is on.
	if server.config.VerifyMessageSignature {
		// Recover the signer to verify that it is the same identity as the one making the RPC call.
		isVerified, recoveredAddress, err := server.VerifyMessageSignature(message, identity)
		if err != nil {
			return fmt.Errorf("error while verifying message: %s", err.Error())
		}
		if !isVerified {
			return fmt.Errorf("recovered signer %s != identity %s", recoveredAddress, identity.Name)
		}
	}

	// For every message verify that the timestamp is within an acceptable drift time.
	messageAge := time.Since(time.Unix(message.Timestamp, 0)).Seconds()
	if messageAge > float64(server.config.MessageDriftTime) {
		return fmt.Errorf("message is older than acceptable drift: %.2f seconds old", messageAge)
	}

	return nil
}

func (server *p2PRelayServer) VerifySufficientBalance(signer *relayp2p.Signer, address string) error {
	// If the flag to verify account balance is turned off, do nothing.
	if !server.config.VerifyAccountBalance {
		return nil
	}

	if signer.ShouldCheckBalance() {
		balance, err := eth.GetCurrentBalance(server.ethClient, address)
		if err != nil {
			return err
		}
		server.logger.Info("fetched up-to-date balance for account", zap.String("address", address), zap.Uint64("balance", balance))

		// Update the "cached" balance on the signer, which helps us know whether to keep checking or not.
		signer.SetHasSufficientBalance(balance > server.config.MinAccountBalance)

		if !signer.HasSufficientBalance() {
			return fmt.Errorf("signer with address %s has insufficient balance (%d wei) to push messages via relay", address, balance)
		}
	} else {
		if !signer.HasSufficientBalance() {
			return fmt.Errorf("signer with address %s has insufficient balance as of last check", address)
		}
	}
	return nil
}

func (server *p2PRelayServer) IsKnownMessage(msgHash []byte) bool {
	return server.knownMessages.Contains(msgHash)
}

func (server *p2PRelayServer) MarkMessage(msgHash []byte) {
	server.knownMessages.Add(msgHash)
}

func (server *p2PRelayServer) P2PStreamHandler(stream network.Stream) {
	server.logger.Info("received new p2p stream")
	err := server.HandlePeerStream(stream)
	if err != nil {
		server.logger.Info("error handling p2p stream", zap.Error(err))
		stream.Reset()
	} else {
		stream.Close()
	}
}

func (server *p2PRelayServer) PeerRecvWorker(peer *relayp2p.Peer, prw *nodep2p.ProtoStream) error {
	for {
		data, err := prw.Read()
		if err != nil {
			return err
		}
		request := &pb.PushRequest{}
		err = proto.Unmarshal(data, request)
		if err != nil {
			return err
		}
		err = server.HandlePeerPushRequest(request)
		if err != nil {
			return err
		}
		// Error rate limit then disconnect [?]
	}
}

func (server *p2PRelayServer) PeerSendWorker(peer *relayp2p.Peer, prw *nodep2p.ProtoStream) error {
	propagatedRequestsChannel := peer.GetChannel()
	for {
		propRequest := <-propagatedRequestsChannel
		if propRequest == nil {
			server.logger.Warn("propagated message is nil")
		} else {
			data, err := proto.Marshal(propRequest)
			if err != nil {
				return err
			}
			prw.Write(data)
		}
	}
	return nil
}

func (server *p2PRelayServer) HandlePeerStream(stream network.Stream) error {
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	prw := &nodep2p.ProtoStream{RW: rw}
	// TODO: use context [?]
	go server.PeerSendWorker(nil, prw)
	return server.PeerRecvWorker(nil, prw)
}

func (server *p2PRelayServer) HandlePeerPushRequest(request *pb.PushRequest) error {
	message := request.Message
	msgHash := server.HashMessage(request.Message)

	if server.IsKnownMessage(msgHash) {
		return nil
	} else {
		server.MarkMessage(msgHash)
	}

	// When pushing a single message, we recover the sender from the message signature, which has
	// different format then identity signature.
	_, recoveredAddress, err := server.VerifyMessageSignature(request.Message, nil)
	if err != nil {
		return err
	}

	// Create an identity object from the address that signed the message.
	identity := &pb.Identity{
		Name: recoveredAddress,
	}

	// Get the signer object for this identity to make sure it's authenticated.
	signer, exists := server.SignerRegistry.GetSignerFromIdentity(identity)
	if !exists {
		server.SignerRegistry.Register(identity, server.config)
	}

	// Check if the signer has a balance. We only propagate messages from signers which
	// sufficient balance. The checks to Ethereum client are rate limited such that we
	// don't check balance on every request.
	err = server.VerifySufficientBalance(signer, recoveredAddress)
	if err != nil {
		server.logger.Warn("signer balance verification failed", zap.Error(err))
		return nil
	}

	// Rate limit the signer, if necessary.
	// if !signer.GetLimiter().Allow() {
	// 	server.logger.Warn("signer rate limited", zap.String("signer", recoveredAddress), zap.Int("max pushed msg/s allowed", server.config.MessageRateLimit))
	// 	return nil
	// }

	// Verify that the message is OK to relayp2p.
	err = server.VerifyMessage(message, identity)
	if err != nil {
		return err
	}

	// Propagate the message.
	// propagate.

	// Update the ping timer on the signer since the signer has just pushed a valid message.
	signer.Ping()

	return nil
}
