package qvmanager

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type QueryViewRecovery interface {
	Recover() ([]*viewpb.QueryViewOfShardAtCoord, error)
	SaveQueryView(qv []*viewpb.QueryViewOfShardAtCoord) error
}
