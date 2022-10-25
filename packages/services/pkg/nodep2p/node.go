package nodep2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"latticexyz/mud/packages/services/pkg/logger"
	mrand "math/rand"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	maddr "github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type Node struct {
	host host.Host
}

func NewP2PNode(listenPort int, seed int64) *Node {
	priv, err := generatePrivateKey(seed)
	if err != nil {
		logger.GetLogger().Fatal("failed to generate p2p host private key", zap.Error(err))
	}

	host, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	)
	if err != nil {
		logger.GetLogger().Fatal("failed to construct new p2p host", zap.Error(err))
	}

	return &Node{host: host}
}

func (node *Node) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	node.host.SetStreamHandler(pid, handler)
}

func (node *Node) OpenStream(addr maddr.Multiaddr, handler network.StreamHandler) error {
	peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		// TODO: add context
		return err
	}
	node.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.PermanentAddrTTL)
	stream, err := node.host.NewStream(context.Background(), peerInfo.ID, "/echo/1.0.0")
	go handler(stream)
	return nil
}

func (node *Node) GetAddress() (maddr.Multiaddr, error) {
	// TODO: handle error
	hostAddr, _ := maddr.NewMultiaddr(fmt.Sprintf("/p2p/%s", node.host.ID().Pretty()))
	addr := node.host.Addrs()[0]
	return addr.Encapsulate(hostAddr), nil
}

func generatePrivateKey(seed int64) (crypto.PrivKey, error) {
	var r io.Reader
	if seed == 0 {
		r = rand.Reader
	} else {
		r = mrand.New(mrand.NewSource(seed))
	}

	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		return nil, err
	}

	return priv, err
}
