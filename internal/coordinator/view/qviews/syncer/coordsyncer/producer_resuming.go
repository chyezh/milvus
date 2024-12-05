package coordsyncer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/service"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

type ResumeSyncClient struct {
	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	mu         sync.Mutex
	workNode   qviews.WorkNode
	syncClinet *service.SyncClient
	recvChan   chan service.SyncResponseFromNode
}

func (c *ResumeSyncClient) SyncAtBackground(req *viewpb.SyncQueryViewsRequest) {
	syncClient := c.getSyncClient()
	if syncClient == nil {
		syncClient.SyncAtBackground(req)
	}
}

func (c *ResumeSyncClient) resumeLoop() {
	for {
		syncClient, err := service.CreateSyncClient(c.notifier.Context(), c.workNode, nil, c.recvChan)
		if err != nil {
			continue
		}
		c.mu.Lock()
		c.syncClinet = syncClient
		c.mu.Unlock()
		syncClient.Execute()
	}
}

func (c *ResumeSyncClient) getSyncClient() *service.SyncClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncClinet
}
