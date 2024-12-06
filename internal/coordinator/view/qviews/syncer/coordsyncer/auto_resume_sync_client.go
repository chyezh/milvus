package coordsyncer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/service"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func newAutoResumeSyncClient(
	workNode qviews.WorkNode,
	recvChan chan service.SyncResponseFromNode,
	resumeChan chan qviews.WorkNode,
) *autoResumeSyncClient {
	c := &autoResumeSyncClient{
		notifier:   syncutil.NewAsyncTaskNotifier[struct{}](),
		mu:         sync.Mutex{},
		workNode:   workNode,
		syncClinet: nil,
		recvChan:   recvChan,
		resumeChan: resumeChan,
	}
	go c.resumeLoop()
	return c
}

type autoResumeSyncClient struct {
	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	mu         sync.Mutex
	workNode   qviews.WorkNode
	syncClinet *service.SyncClient
	recvChan   chan service.SyncResponseFromNode
	resumeChan chan qviews.WorkNode
}

// SyncAtBackground syncs the query views at background.
func (c *autoResumeSyncClient) SyncAtBackground(req *viewpb.SyncQueryViewsRequest) {
	syncClient := c.getSyncClient()
	if syncClient == nil {
		syncClient.SyncAtBackground(req)
	}
}

func (c *autoResumeSyncClient) resumeLoop() {
	for {
		syncClient, err := service.CreateSyncClient(c.notifier.Context(), c.workNode, nil, c.recvChan)
		if err != nil {
			continue
		}
		if old := c.swapSyncClient(syncClient); old != nil {
			old.Close()
		}
		syncClient.Execute()
		c.resumeChan <- c.workNode
	}
}

// swapSyncClient swaps the current sync client with the new sync client.
func (c *autoResumeSyncClient) swapSyncClient(syncClient *service.SyncClient) *service.SyncClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.syncClinet
	c.syncClinet = syncClient
	return old
}

func (c *autoResumeSyncClient) getSyncClient() *service.SyncClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncClinet
}

func (c *autoResumeSyncClient) Close() {
	c.notifier.Cancel()
	c.notifier.BlockUntilFinish()
	old := c.swapSyncClient(nil)
	old.Close()
}
