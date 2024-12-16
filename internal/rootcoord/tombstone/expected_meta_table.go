package tombstone

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ExpectedMetaTable interface {
	GetCollectionByID(ctx context.Context, dbName string, collectionID typeutil.UniqueID, ts typeutil.Timestamp, allowUnavailable bool) (*model.Collection, error)
}
