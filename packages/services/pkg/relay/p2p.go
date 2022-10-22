package relay

type P2PRelayServerConfig struct {
	MessageDriftTime  int
	MinAccountBalance uint64

	VerifyMessageSignature bool
	VerifyAccountBalance   bool
}
