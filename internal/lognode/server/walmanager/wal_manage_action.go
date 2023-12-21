package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
)

const (
	walManageActionOpen   = 1
	walManageActionRemove = 2
)

type walManageAction struct {
	ctx         context.Context
	action      int
	allocateOpt *wal.OpenOption
	term        int64
	result      chan error
}

type walManageActions []*walManageAction

func (a walManageActions) Len() int {
	return len(a)
}

func (a walManageActions) Less(i, j int) bool {
	return a[i].term < a[j].term || (a[i].term == a[j].term && a[i].action == walManageActionOpen)
}

func (a walManageActions) Swap(i, j int) {
	tmp := a[i]
	a[i] = a[j]
	a[j] = tmp
}
