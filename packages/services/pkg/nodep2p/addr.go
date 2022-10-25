package nodep2p

import (
	"strings"

	maddr "github.com/multiformats/go-multiaddr"
)

func StringsToAddrs(addrStrings []string) ([]maddr.Multiaddr, error) {
	var al []maddr.Multiaddr
	for _, addrStr := range addrStrings {
		addr, err := maddr.NewMultiaddr(addrStr)
		if err != nil {
			return al, err
		}
		al = append(al, addr)
	}
	return al, nil
}

// A new type we need for writing a custom flag parser
type addrList []maddr.Multiaddr

func (al *addrList) String() string {
	addrStrings := make([]string, len(*al))
	for i, addr := range *al {
		addrStrings[i] = addr.String()
	}
	return strings.Join(addrStrings, ",")
}

func (al *addrList) Set(value string) error {
	addr, err := maddr.NewMultiaddr(value)
	if err != nil {
		return err
	}
	*al = append(*al, addr)
	return nil
}
