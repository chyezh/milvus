package client

import (
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	ErrClosed       = errors.New("client is actively closed")    // When the client is actively closed, the error will be returned.
	ErrStreamBroken = errors.New("underlying stream is broken")  // When the underlying stream is broken, the error will be returned, it can be recovery.
	ErrNodeGone     = errors.New("underlying work node is gone") // When the underlying work node is gone, the error will be returned, it can never be recovery.

	_ SyncMessage = SyncResponseMessage{}
	_ SyncMessage = SyncErrorMessage{}
)

type SyncMessage interface {
	WorkNode() qviews.WorkNode
}

type SyncResponseMessage struct {
	workNode qviews.WorkNode
	Response *viewpb.SyncQueryViewsResponse
}

func (m SyncResponseMessage) WorkNode() qviews.WorkNode {
	return m.workNode
}

type SyncErrorMessage struct {
	workNode qviews.WorkNode
	Error    error
}

func (m SyncErrorMessage) WorkNode() qviews.WorkNode {
	return m.workNode
}

// SyncOption is the option to create syncer.
type SyncOption struct {
	WorkNode qviews.WorkNode
	// Receiver is the channel to receive the various message from the node.
	// When the sync operation is done or error happens, the SyncErrorMessage will be received.
	// SyncResponseMessage -> SyncResponseMessage -> SyncErrorMessage.
	Receiver chan<- SyncMessage
}

// QueryViewServiceClient is the interface to send sync request to the view service.
type QueryViewServiceClient interface {
	// Sync create a syncer to send sync request to the related node.
	// Various goroutines will be created to handle the sync operation at background.
	Sync(opt SyncOption) QueryViewServiceSyncer

	// Close the client.
	Close()
}

// QueryViewServiceSyncer is the interface to send sync request to the view service.
// And keep receiving the response from the node.
type QueryViewServiceSyncer interface {
	// SyncAtBackground sent the sync request to the related node.
	// But it doesn't promise the sync operation is done at server-side.
	// Make sure the sync operation is done by the Receiving message.
	SyncAtBackground(*viewpb.SyncQueryViewsRequest)

	// Close the client.
	Close()
}
