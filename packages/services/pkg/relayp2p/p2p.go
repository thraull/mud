package relayp2p

import (
	"fmt"
	pb "latticexyz/mud/packages/services/protobuf/go/ecs-relay"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

type P2PRelayServerConfig struct {
	MessageDriftTime       int
	IdleDisconnectInterval int
	MinAccountBalance      uint64
	VerifyMessageSignature bool
	VerifyAccountBalance   bool
}

type Signer struct {
	idleTimeoutTime     int
	identity            *pb.Identity
	latestPingTimestamp int64

	sufficientBalance          bool
	sufficientBalanceLimiter   *rate.Limiter
	insufficientBalanceLimiter *rate.Limiter
	// messageRateLimiter         *rate.Limiter

	mutex sync.Mutex
}

func (signer *Signer) Ping() {
	signer.mutex.Lock()
	signer.latestPingTimestamp = time.Now().Unix()
	signer.mutex.Unlock()
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
	signers []*Signer
	mutex   sync.Mutex
}

func (registry *SignerRegistry) Count() int {
	return len(registry.signers)
}

func (registry *SignerRegistry) GetSigners() []*Signer {
	return registry.signers
}

func (registry *SignerRegistry) GetSignerFromIdentity(identity *pb.Identity) (*Signer, bool) {
	registry.mutex.Lock()
	for _, signer := range registry.signers {
		if signer.identity.Name == identity.Name {
			registry.mutex.Unlock()
			return signer, true
		}
	}
	registry.mutex.Unlock()
	return nil, false
}

func (registry *SignerRegistry) Register(identity *pb.Identity, config *P2PRelayServerConfig) {
	registry.mutex.Lock()

	newSigner := new(Signer)
	newSigner.identity = identity
	// newSigner.messageRateLimiter = rate.NewLimiter(rate.Every(1*time.Second/time.Duration(config.MessageRateLimit)), config.MessageRateLimit)
	// At most allow a check for balance every 300s if signer has funds and every 60s if not.
	newSigner.sufficientBalanceLimiter = rate.NewLimiter(rate.Limit(float64(1)/float64(300)), 1)
	newSigner.insufficientBalanceLimiter = rate.NewLimiter(rate.Limit(float64(1)/float64(60)), 1)
	newSigner.sufficientBalance = false

	registry.signers = append(registry.signers, newSigner)
	registry.mutex.Unlock()
}

func (registry *SignerRegistry) Unregister(identity *pb.Identity) error {
	registry.mutex.Lock()
	for index, signer := range registry.signers {
		if signer.identity.Name == identity.Name {
			registry.signers = append(registry.signers[:index], registry.signers[index+1:]...)
			registry.mutex.Unlock()
			return nil
		}
	}
	registry.mutex.Unlock()
	return fmt.Errorf("signer not registered")
}

// Relay server connected to the p2p
type Client struct {
	id      string
	channel chan *pb.PushRequest
	mutex   sync.Mutex
}

func (client *Client) GetId() string {
	return client.id
}

func (client *Client) GetChannel() chan *pb.PushRequest {
	return client.channel
}

type ClientRegistry struct {
	clients []*Client
	mutex   sync.Mutex
}

func (registry *ClientRegistry) Count() int {
	return len(registry.clients)
}

func (registry *ClientRegistry) GetClients() []*Client {
	return registry.clients
}

func (registry *ClientRegistry) GetClientFromId(id string) (*Client, error) {
	registry.mutex.Lock()
	for _, client := range registry.clients {
		if client.id == id {
			registry.mutex.Unlock()
			return client, nil
		}
	}
	registry.mutex.Unlock()
	return nil, fmt.Errorf("client not registered: %s", id)
}

func (registry *ClientRegistry) NewClient(id string, config *P2PRelayServerConfig) *Client {
	registry.mutex.Lock()
	newClient := new(Client)
	newClient.id = id
	newClient.channel = make(chan *pb.PushRequest)
	registry.clients = append(registry.clients, newClient)
	registry.mutex.Unlock()
	return newClient
}

func (registry *ClientRegistry) RemoveClient(id string) error {
	registry.mutex.Lock()
	for index, client := range registry.clients {
		if client.id == id {
			registry.clients = append(registry.clients[:index], registry.clients[index+1:]...)
			registry.mutex.Unlock()
			return nil
		}
	}
	registry.mutex.Unlock()
	return fmt.Errorf("client not registered")
}

func (registry *ClientRegistry) Propagate(request *pb.PushRequest, origin string) {
	registry.mutex.Lock()
	for _, client := range registry.clients {
		// Only pipe to clients that are connected and not the client which is the origin of
		// the request.
		client.mutex.Lock()
		if client.id != origin {
			client.channel <- request
		}
		client.mutex.Unlock()
	}
	registry.mutex.Unlock()
}

type Peer struct {
	id      *peer.ID
	channel chan *pb.PushRequest
	mutex   sync.Mutex
	// Rate limiting
}

func (peer *Peer) GetChannel() chan *pb.PushRequest {
	return peer.channel
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

func (registry *PeerRegistry) GetPeerFromID(id *peer.ID) (*Peer, error) {
	registry.mutex.Lock()
	for _, peer := range registry.peers {
		if peer.id == id {
			registry.mutex.Unlock()
			return peer, nil
		}
	}
	registry.mutex.Unlock()
	return nil, fmt.Errorf("peer not registered: %s", id.String())
}

func (registry *PeerRegistry) Register(id *peer.ID, config *P2PRelayServerConfig) {
	registry.mutex.Lock()
	newPeer := new(Peer)
	newPeer.id = id
	newPeer.channel = make(chan *pb.PushRequest)
	registry.peers = append(registry.peers, newPeer)
	registry.mutex.Unlock()
}

func (registry *PeerRegistry) Unregister(id *peer.ID) error {
	registry.mutex.Lock()
	for index, peer := range registry.peers {
		if peer.id == id {
			registry.peers = append(registry.peers[:index], registry.peers[index+1:]...)
			registry.mutex.Unlock()
			return nil
		}
	}
	registry.mutex.Unlock()
	return fmt.Errorf("peer not registered")
}

func (registry *PeerRegistry) Propagate(request *pb.PushRequest, origin *peer.ID) {
	registry.mutex.Lock()
	for _, client := range registry.peers {
		// Only pipe to peers that are connected and not the client which is the origin of
		// the request.
		client.mutex.Lock()
		if client.id != origin {
			client.channel <- request
		}
		client.mutex.Unlock()
	}
	registry.mutex.Unlock()
}
