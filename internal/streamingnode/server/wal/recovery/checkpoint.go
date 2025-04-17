package recovery

const (
	recoveryMagicStreamingInitialized int64 = iota // the vchannel info is set into the catalog.
	// the checkpoint is set into the catalog.
)
