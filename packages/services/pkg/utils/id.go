package utils

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func GenerateRandomIdentifier() (string, error) {
	timestamp, err := time.Now().MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("cannot generated random identifier")
	}
	return crypto.Keccak256Hash(timestamp).Hex(), nil
}
