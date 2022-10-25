package grpc

import (
	"bufio"
	"context"
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
	libp2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type p2PRelayServer struct {
	pb.UnimplementedP2PRelayServiceServer
	relayp2p.SignerRegistry
	relayp2p.PeerRegistry
	// Support a single relay client
	relayp2p.Client

	ethClient     *ethclient.Client
	nodep2p       host.Host
	config        *relayp2p.P2PRelayServerConfig
	knownMessages *utils.KnownCache
	logger        *zap.Logger
}

func (server *p2PRelayServer) Init() {
	// TODO: do the math on cache size (and vs. BloomFilter).
	// Init the cache of known message hashes.
	server.knownMessages = utils.NewKnownCache(1048576)
	// Set up the stream handler.
	server.nodep2p.SetStreamHandler("/relay/0.1.0", server.P2PStreamHandler)
}

// Opened by relay clients to get data streamed back.
func (server *p2PRelayServer) OpenStream(_ *emptypb.Empty, stream pb.P2PRelayService_OpenStreamServer) error {
	err := server.Client.ConnectOpenStream()
	if err != nil {
		server.logger.Info("error opening open write stream to relay client", zap.Error(err))
		return err
	}
	server.logger.Info("opened write stream to relay client")
	defer server.logger.Info("closed write stream to relay client")
	defer server.Client.DisconnectOpenStream()

	// Stream propagated data to the client stream
	propagatedRequestsChannel := server.Client.GetChannel()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case relayedRequests := <-propagatedRequestsChannel:
			err = stream.Send(relayedRequests)
			if err != nil {
				server.logger.Info("error sending push request to relay client", zap.Error(err))
				return err
			}
		}
	}
}

// Opened by relay clients to stream data into the node.
func (server *p2PRelayServer) PushStream(stream pb.P2PRelayService_PushStreamServer) error {
	err := server.Client.ConnectPushStream()
	if err != nil {
		server.logger.Info("error opening read stream from relay client", zap.Error(err))
		return err
	}
	server.logger.Info("opened read stream from relay client")
	defer server.logger.Info("closed read stream from relay client")
	defer server.Client.DisconnectPushStream()

	// Continuously receive message relay requests and handle them.
	for {
		// Receive request message from input stream.
		request, err := stream.Recv()
		if err == io.EOF {
			m := new(emptypb.Empty)
			stream.SendAndClose(m)
			return nil
		} else if err != nil {
			server.logger.Info("error receiving push request from relay client", zap.Error(err))
			return err
		}
		// TODO: disconnect if (errors/s)/(requests/s) exceeds limit.
		err = server.HandleClientPushRequest(request)
		if err != nil {
			server.logger.Info("error handling push request from relay client", zap.Error(err))
			return err
		}
	}
}

func (server *p2PRelayServer) HandleClientPushRequest(request *pb.PushRequest) error {
	shouldPropagate, err := server.ShouldPropagate(request)
	if !shouldPropagate || err != nil {
		return err
	}
	// Propagate to other peers.
	var zeroPeerId libp2p_peer.ID
	server.PeerRegistry.Propagate(request, zeroPeerId)
	// Don't propagate to clients as we currently only support one, which is always
	// the origin of the request.
	return nil
}

// TODO: Reputation system, give more compute to valuable peers

func (server *p2PRelayServer) P2PStreamHandler(stream network.Stream) {
	err := server.HandlePeerStream(stream)
	if err != nil {
		server.logger.Info("error handling p2p stream", zap.Error(err))
		stream.Reset()
	} else {
		stream.Close()
	}
}

func (server *p2PRelayServer) HandlePeerStream(stream network.Stream) error {
	// Get and authorize peer.
	// TODO: peerId typing and pointer-ing
	peerId := stream.Conn().RemotePeer()
	allowed := server.AllowPeer(peerId)
	if !allowed {
		return fmt.Errorf("peer with id=%s is blocked", peerId.ShortString())
	}
	// TODO: use multiple streams per peer to mitigate head of the line blocking.
	// Allow only one stream per peer.
	_, exists := server.PeerRegistry.GetPeerFromId(peerId)
	if exists {
		return fmt.Errorf("peer with id=%s already has an open stream", peerId.ShortString())
	}
	// Add peer.
	peer := server.PeerRegistry.AddPeer(peerId, server.config)
	defer server.PeerRegistry.RemovePeer(peerId)
	server.logger.Info("received new p2p stream", zap.String("peerId", peerId.ShortString()))

	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	prw := &nodep2p.ProtoStream{RW: rw}

	// Start I/O workers.
	ctx, cancel := context.WithCancel(context.Background())
	go server.PeerRecvWorker(cancel, ctx, peer, prw)
	go server.PeerSendWorker(cancel, ctx, peer, prw)

	// Wait for I/O to be over.
	<-ctx.Done()

	// I/O is only closed by an error so the function always returns a generic error and
	// logs the specific one from the worker.
	return fmt.Errorf("error handling peer stream")
}

func (server *p2PRelayServer) PeerRecvWorker(cancel context.CancelFunc, ctx context.Context, peer *relayp2p.Peer, prw *nodep2p.ProtoStream) error {
	defer cancel()
	for {
		select {
		case <-ctx.Done():
		default:
		}
		data, err := prw.Read()
		if err != nil {
			server.logger.Info("error reading peer stream", zap.Error(err))
			return err
		}
		request := &pb.PushRequest{}
		err = proto.Unmarshal(data, request)
		if err != nil {
			server.logger.Info("error unmarshal-ing peer stream data", zap.Error(err))
			return err
		}
		err = server.HandlePeerPushRequest(request, peer)
		if err != nil {
			server.logger.Info("error handling peer push request", zap.Error(err))
			return err
		}
	}
}

func (server *p2PRelayServer) PeerSendWorker(cancel context.CancelFunc, ctx context.Context, peer *relayp2p.Peer, prw *nodep2p.ProtoStream) error {
	propagatedRequestsChannel := peer.GetChannel()
	defer cancel()
	for {
		select {
		case <-ctx.Done():
		case request := <-propagatedRequestsChannel:
			data, err := proto.Marshal(request)
			if err != nil {
				server.logger.Info("error marshaling peer push request", zap.Error(err))
				return err
			}
			err = prw.Write(data)
			if err != nil {
				server.logger.Info("error reading to peer stream", zap.Error(err))
				return err
			}
		}
	}
}

func (server *p2PRelayServer) HandlePeerPushRequest(request *pb.PushRequest, peer *relayp2p.Peer) error {
	shouldPropagate, err := server.ShouldPropagate(request)
	if !shouldPropagate || err != nil {
		return err
	}
	// Propagate to the client(s).
	server.Client.Propagate(request, "")
	// Propagate to peers.
	server.PeerRegistry.Propagate(request, peer.GetId())
	return nil
}

func (server *p2PRelayServer) AllowPeer(id libp2p_peer.ID) bool {
	return true
}

func (server *p2PRelayServer) ShouldPropagate(request *pb.PushRequest) (bool, error) {
	message := request.Message
	msgHash := server.HashMessage(request.Message)

	if server.IsKnownMessage(msgHash) {
		return false, nil
	} else {
		server.MarkMessage(msgHash)
	}

	// When pushing a single message, we recover the sender from the message signature, which has
	// different format then identity signature.
	_, recoveredAddress, err := server.VerifyMessageSignature(message, nil)
	if err != nil {
		return false, err
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
		return false, fmt.Errorf("error verifying signer balance: %s", err.Error())
	}

	// Rate limit the signer, if necessary.
	// if !signer.GetLimiter().Allow() {
	// 	server.logger.Warn("signer rate limited", zap.String("signer", recoveredAddress), zap.Int("max pushed msg/s allowed", server.config.MessageRateLimit))
	// 	return nil
	// }

	// Verify that the message is OK to relayp2p.
	err = server.VerifyMessage(message, identity)
	if err != nil {
		return false, err
	}

	// Update the ping timer on the signer since the signer has just pushed a valid message.
	signer.Ping()

	return true, nil
}

// TODO: move these functions to avoid repetition with relay.

func (server *p2PRelayServer) EncodeMessage(message *pb.Message) string {
	return fmt.Sprintf("(%d,%s,%s,%d)", message.Version, message.Id, crypto.Keccak256Hash(message.Data).Hex(), message.Timestamp)
}

func (server *p2PRelayServer) HashMessage(message *pb.Message) []byte {
	messagePacked := server.EncodeMessage(message)
	hash := accounts.TextHash([]byte(messagePacked))
	return hash
}

func (server *p2PRelayServer) IsKnownMessage(msgHash []byte) bool {
	return server.knownMessages.Contains(msgHash)
}

func (server *p2PRelayServer) MarkMessage(msgHash []byte) {
	server.knownMessages.Add(msgHash)
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
			return fmt.Errorf("error verifying message: %s", err.Error())
		}
		if !isVerified {
			return fmt.Errorf("recovered signer %s != identity %s", recoveredAddress, identity.Name)
		}
	}

	// For every message verify that the timestamp is within an acceptable drift time.
	messageAge := time.Since(time.Unix(message.Timestamp, 0)).Seconds()
	if messageAge > float64(server.config.MessageDriftTime) {
		return fmt.Errorf("message older than acceptable drift: %.2f seconds old", messageAge)
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
