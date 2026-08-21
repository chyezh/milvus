package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"
	DirectoryTransformLog  = "tl"
	DirectoryVChannel      = "vchannel"
	DirectorySchema        = "schema"
	DirectorySummaryStore  = "summary-store"

	KeyConsumeCheckpoint   = "consume-checkpoint"
	KeyRecoveryControl     = "recovery-control"
	KeySalvageCheckpoint   = "salvage-checkpoint"
	KeyPChannelSummaryMeta = "pchannel-summary-meta"
)
