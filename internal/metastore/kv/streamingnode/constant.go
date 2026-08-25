package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"
	DirectoryVChannel      = "vchannel"
	DirectorySchema        = "schema"
	DirectorySummaryStore  = "summary-store"
	DirectoryPendingL0     = "pending-l0"

	KeyConsumeCheckpoint = "consume-checkpoint"
	KeyRecoveryControl   = "recovery-control"
	KeySalvageCheckpoint = "salvage-checkpoint"
)
