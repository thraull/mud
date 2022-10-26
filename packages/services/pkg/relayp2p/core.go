package relayp2p

import (
	"fmt"
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"
	"sync"
	"time"

	libp2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// TODO: abstract config with relay

type P2PRelayServerConfig struct {
	MessageDriftTime       int
	IdleDisconnectInterval int
	MinAccountBalance      uint64
	VerifyMessageSignature bool
	VerifyAccountBalance   bool
}

type Signer struct {
	identity            *pb.Identity
	idleTimeoutTime     int
	latestPingTimestamp int64

	sufficientBalance          bool
	sufficientBalanceLimiter   *rate.Limiter
	insufficientBalanceLimiter *rate.Limiter
	// messageRateLimiter         *rate.Limiter

	mutex sync.Mutex
}

func (signer *Signer) Ping() {
	signer.mutex.Lock()
	defer signer.mutex.Unlock()
	signer.latestPingTimestamp = time.Now().Unix()
}

func (signer *Signer) IsIdle(idleTimeoutTime int) bool {
	return time.Since(time.Unix(signer.latestPingTimestamp, 0)).Seconds() >= float64(idleTimeoutTime)
}

func (signer *Signer) GetIdentity() *pb.Identity {
	return signer.identity
}

// func (signer *Signer) GetLimiter() *rate.Limiter {
// 	return signer.messageRateLimiter
// }

func (signer *Signer) HasSufficientBalance() bool {
	return signer.sufficientBalance
}

func (signer *Signer) SetHasSufficientBalance(hasSufficientBalance bool) {
	signer.sufficientBalance = hasSufficientBalance
}

func (signer *Signer) ShouldCheckBalance() bool {
	if signer.sufficientBalance {
		return signer.sufficientBalanceLimiter.Allow()
	} else {
		return signer.insufficientBalanceLimiter.Allow()
	}
}

type SignerRegistry struct {
	// TODO: make this a cache [?]
	signers []*Signer
	mutex   sync.Mutex
}

func (registry *SignerRegistry) Count() int {
	return len(registry.signers)
}

func (registry *SignerRegistry) GetSignerFromIdentity(identity *pb.Identity) (*Signer, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, signer := range registry.signers {
		if signer.identity.Name == identity.Name {
			return signer, true
		}
	}
	return nil, false
}

func (registry *SignerRegistry) AddSigner(identity *pb.Identity, config *P2PRelayServerConfig) *Signer {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	newSigner := new(Signer)
	newSigner.identity = identity
	// newSigner.messageRateLimiter = rate.NewLimiter(rate.Every(1*time.Second/time.Duration(config.MessageRateLimit)), config.MessageRateLimit)
	// At most allow a check for balance every 300s if signer has funds and every 60s if not.
	newSigner.sufficientBalanceLimiter = rate.NewLimiter(rate.Limit(float64(1)/float64(300)), 1)
	newSigner.insufficientBalanceLimiter = rate.NewLimiter(rate.Limit(float64(1)/float64(60)), 1)
	newSigner.sufficientBalance = false

	registry.signers = append(registry.signers, newSigner)
	return newSigner
}

func (registry *SignerRegistry) RemoveSigner(identity *pb.Identity) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for index, signer := range registry.signers {
		if signer.identity.Name == identity.Name {
			registry.signers = append(registry.signers[:index], registry.signers[index+1:]...)
			return nil
		}
	}
	return fmt.Errorf("signer identity.Name=%s not registered", identity.Name)
}

// Relay server connected to the p2p
type Client struct {
	channel   chan *pb.PushRequest
	receiving bool
	sending   bool
	mutex     sync.Mutex
}

func NewClient(config *P2PRelayServerConfig) *Client {
	newClient := new(Client)
	newClient.channel = make(chan *pb.PushRequest)
	newClient.sending = false
	newClient.receiving = false
	return newClient
}

func (client *Client) GetChannel() chan *pb.PushRequest {
	return client.channel
}

func (client *Client) IsReceiving() bool {
	return client.receiving
}

func (client *Client) IsSending() bool {
	return client.sending
}

// TODO: abstract this for simplicity

func (client *Client) ConnectOpenStream() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if client.IsReceiving() {
		return fmt.Errorf("open stream already connected")
	}
	client.receiving = true
	return nil
}

func (client *Client) DisconnectOpenStream() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if !client.IsReceiving() {
		return fmt.Errorf("open stream not connected")
	}
	client.receiving = false
	return nil
}

func (client *Client) ConnectPushStream() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if client.IsSending() {
		return fmt.Errorf("push stream already connected")
	}
	client.sending = true
	return nil
}

func (client *Client) DisconnectPushStream() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if !client.IsSending() {
		return fmt.Errorf("push stream not connected")
	}
	client.sending = false
	return nil
}

func (client *Client) Propagate(request *pb.PushRequest, origin string) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	client.channel <- request
}

type Peer struct {
	id      libp2p_peer.ID
	channel chan *pb.PushRequest
	mutex   sync.Mutex
	// Rate limiting
}

func (peer *Peer) GetChannel() chan *pb.PushRequest {
	return peer.channel
}

func (peer *Peer) GetId() libp2p_peer.ID {
	return peer.id
}

type PeerRegistry struct {
	peers []*Peer
	mutex sync.Mutex
}

func (registry *PeerRegistry) Count() int {
	return len(registry.peers)
}

func (registry *PeerRegistry) GetPeers() []*Peer {
	return registry.peers
}

func (registry *PeerRegistry) GetPeerFromId(id libp2p_peer.ID) (*Peer, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, peer := range registry.peers {
		if peer.id == id {
			return peer, true
		}
	}
	return nil, false
}

func (registry *PeerRegistry) AddPeer(id libp2p_peer.ID, config *P2PRelayServerConfig) *Peer {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	newPeer := new(Peer)
	newPeer.id = id
	newPeer.channel = make(chan *pb.PushRequest)
	registry.peers = append(registry.peers, newPeer)
	return newPeer
}

func (registry *PeerRegistry) RemovePeer(id libp2p_peer.ID) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for index, peer := range registry.peers {
		if peer.id == id {
			registry.peers = append(registry.peers[:index], registry.peers[index+1:]...)
			return nil
		}
	}
	return fmt.Errorf("peer id=%s not registered", id.ShortString())
}

func (registry *PeerRegistry) Propagate(request *pb.PushRequest, origin libp2p_peer.ID) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, peer := range registry.peers {
		// Only pipe to peers that are connected and not the peer which is the origin of
		// the request.
		peer.mutex.Lock()
		if peer.id != origin {
			peer.channel <- request
		}
		peer.mutex.Unlock()
	}
}
