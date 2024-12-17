package tombstone

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

const defaultTombstoneTTL = 7 * 24 * time.Hour

type (
	DroppingCollection = datapb.CollectionTombstoneImpl
	DroppingPartition  = datapb.PartitionTombstoneImpl
)

// collectionTombstoneImpl is a tombstone that is used to mark a resource as dropping.
// It is used to perform drop operation atomic and idempotent between coords.
// TODO: It can be removed after we have a global unique coordination mechanism (merge all coord into one).
type collectionTombstoneImpl struct{}

// IsPartitionDropped checks if a partition is dropped.
func (dt *collectionTombstoneImpl) CheckIfPartitionDropped(ctx context.Context, collectionID int64, partitionID int64) error {
}

func (dt *collectionTombstoneImpl) CheckIfVChannelDropped(vchannel string) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if collectionID, ok := dt.virtualChannels.Get(vchannel); ok {
		return merr.WrapErrCollectionDropped(collectionID)
	}
	return nil
}
