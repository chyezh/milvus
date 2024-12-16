package tombstone

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// collectionTombstoneImpl is a tombstone that is used to mark a resource as dropping.
// It is used to perform drop operation atomic and idempotent between coords.
// TODO: It can be removed after we have a global unique coordination mechanism (merge all coord into one).
type collectionTombstoneImpl struct {
	innerTabl *syncutil.Future[ExpectedMetaTable]
}

// CheckIfCollectionDrop checks if a collection is dropping or dropped.
func (dt collectionTombstoneImpl) CheckIfCollectionDrop(ctx context.Context, collectionID int64) error {
	tbl, err := dt.innerTabl.GetWithContext(ctx)
	if err != nil {
		return err
	}
	coll, err := tbl.GetCollectionByID(ctx, "", collectionID, typeutil.MaxTimestamp, true)

	return dt.checkIfCollectionDrop(coll, err)
}

// CheckIfPartitionDrop checks if a partition is dropping or dropped.
func (dt collectionTombstoneImpl) CheckIfPartitionDrop(ctx context.Context, collectionID int64, partitionID int64) error {
	tbl, err := dt.innerTabl.GetWithContext(ctx)
	if err != nil {
		return err
	}
	coll, err := tbl.GetCollectionByID(ctx, "", collectionID, typeutil.MaxTimestamp, true)

	if err := dt.checkIfCollectionDrop(coll, err); err != nil {
		return err
	}
	for _, partition := range coll.Partitions {
		if partition.PartitionID == partitionID {
			return dt.checkIfPartitionDrop(partition)
		}
	}
	return merr.WrapErrPartitionDrop(partitionID)
}

func (dt collectionTombstoneImpl) checkIfCollectionDrop(coll *model.Collection, err error) error {
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return merr.WrapErrCollectionDrop(coll.CollectionID)
	}
	if coll.State == pb.CollectionState_CollectionDropping || coll.State == pb.CollectionState_CollectionDropped {
		return merr.WrapErrCollectionDrop(coll.CollectionID)
	}
	return nil
}

func (dt collectionTombstoneImpl) checkIfPartitionDrop(partition *model.Partition) error {
	if partition.State == pb.PartitionState_PartitionDropping || partition.State == pb.PartitionState_PartitionDropped {
		return merr.WrapErrPartitionDrop(partition.PartitionID)
	}
	return nil
}
