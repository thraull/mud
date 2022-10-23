package utils

import (
	mapset "github.com/deckarep/golang-set"
)

// https://github.com/ethereum/go-ethereum/blob/master/eth/protocols/eth/peer.go

// max is a helper function which returns the larger of the two given integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type KnownCache struct {
	hashes mapset.Set
	max    int
}

// NewKnownCache creates a new KnownCache with a max capacity.
func NewKnownCache(max int) *KnownCache {
	return &KnownCache{
		max:    max,
		hashes: mapset.NewSet(),
	}
}

// Add adds a list of elements to the set.
func (k *KnownCache) Add(hashes ...[]byte) {
	for k.hashes.Cardinality() > max(0, k.max-len(hashes)) {
		k.hashes.Pop()
	}
	for _, hash := range hashes {
		k.hashes.Add(hash)
	}
}

// Contains returns whether the given item is in the set.
func (k *KnownCache) Contains(hash []byte) bool {
	return k.hashes.Contains(hash)
}

// Cardinality returns the number of elements in the set.
func (k *KnownCache) Cardinality() int {
	return k.hashes.Cardinality()
}
