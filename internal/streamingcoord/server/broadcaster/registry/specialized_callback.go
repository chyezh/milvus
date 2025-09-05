package registry

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// init the message ack callbacks
func init() {
	resetMessageAckCallbacks()
	resetMessageCheckCallbacks()
}

var RegisterImportMessageV1CheckCallback = registerMessageCheckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]

// resetMessageCheckCallbacks resets the message check callbacks.
func resetMessageCheckCallbacks() {
	messageCheckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerCheckCallback]{
		message.MessageTypeImportV1: syncutil.NewFuture[messageInnerCheckCallback](),
	}
}

var (
	RegisterDropPartitionMessageV1AckCallback = registerMessageAckCallback[*message.DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	RegisterImportMessageV1AckCallback        = registerMessageAckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]

	RegisterPutCollectionV2AckCallback         = registerMessageAckCallback[*message.PutCollectionMessageHeader, *message.PutCollectionMessageBody]
	RegisterCreateCollectionV1AckCallback      = registerMessageAckCallback[*message.CreateCollectionMessageHeader, *message.CreateCollectionRequest]
	RegisterDropCollectionV1AckCallback        = registerMessageAckCallback[*message.DropCollectionMessageHeader, *message.DropCollectionRequest]
	RegisterPutLoadConfigMessageV2AckCallback  = registerMessageAckCallback[*message.PutLoadConfigMessageHeader, *message.PutLoadConfigMessageBody]
	RegisterDropLoadConfigMessageV2AckCallback = registerMessageAckCallback[*message.DropLoadConfigMessageHeader, *message.DropLoadConfigMessageBody]

	RegisterPutReplicateConfigV2AckCallback = registerMessageAckCallback[*message.PutReplicateConfigMessageHeader, *message.PutReplicateConfigMessageBody]

	RegisterCreateIndexMessageV2AckCallback = registerMessageAckCallback[*message.CreateIndexMessageHeader, *message.CreateIndexMessageBody]
)

// resetMessageAckCallbacks resets the message ack callbacks.
func resetMessageAckCallbacks() {
	messageAckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckCallback]{
		message.MessageTypeDropPartitionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeImportV1:        syncutil.NewFuture[messageInnerAckCallback](),

		message.MessageTypePutCollectionV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeCreateCollectionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropCollectionV1:   syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutLoadConfigV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropLoadConfigV2:   syncutil.NewFuture[messageInnerAckCallback](),

		message.MessageTypePutReplicateConfigV2: syncutil.NewFuture[messageInnerAckCallback](),

		message.MessageTypeCreateIndexV2: syncutil.NewFuture[messageInnerAckCallback](),
	}
}
