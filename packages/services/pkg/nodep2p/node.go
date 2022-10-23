package nodep2p

import (
	"crypto/rand"
	"fmt"
	"io"
	"latticexyz/mud/packages/services/pkg/logger"
	mrand "math/rand"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/zap"
)

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

func NewP2PNode(listenPort int, seed int64) host.Host {
	priv, err := generatePrivateKey(seed)
	if err != nil {
		logger.GetLogger().Fatal("failed to generate p2p node private key", zap.Error(err))
	}

	node, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	)
	if err != nil {
		logger.GetLogger().Fatal("failed to construct new p2p node", zap.Error(err))
	}

	return node
}
