package walevent

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c Cursor) IntoProto() *streamingpb.WALReplicaCursor {
	return &streamingpb.WALReplicaCursor{Term: c.Term, Seq: c.Seq}
}

func CursorFromProto(c *streamingpb.WALReplicaCursor) Cursor {
	return Cursor{Term: c.GetTerm(), Seq: c.GetSeq()}
}

// IntoProto owns its metadata. Serializing a journal entry must not expose the
// journal's mutable protobuf pointers to transport implementations.
func (e Event) IntoProto() *streamingpb.WALReplicaEvent {
	result := &streamingpb.WALReplicaEvent{Cursor: e.Cursor.IntoProto()}
	payload, _ := e.Payload.freeze()
	switch p := payload.(type) {
	case WALEntry:
		entry := &streamingpb.WALReplicaEntry{Message: proto.CloneOf(p.Message.IntoImmutableMessageProto())}
		if txn := message.AsImmutableTxnMessage(p.Message); txn != nil {
			entry.Begin = proto.CloneOf(txn.Begin().IntoImmutableMessageProto())
			entry.Commit = proto.CloneOf(txn.Commit().IntoImmutableMessageProto())
			_ = txn.RangeOver(func(msg message.ImmutableMessage) error {
				entry.Body = append(entry.Body, proto.CloneOf(msg.IntoImmutableMessageProto()))
				return nil
			})
		}
		result.Payload = &streamingpb.WALReplicaEvent_WalEntry{WalEntry: entry}
	case QueryViewIntentBatch:
		result.Payload = &streamingpb.WALReplicaEvent_QueryViewIntents{QueryViewIntents: &streamingpb.WALReplicaQueryViewIntents{Views: p.Views}}
	case StoragePublished:
		result.Payload = &streamingpb.WALReplicaEvent_StoragePublished{StoragePublished: &streamingpb.WALReplicaStoragePublication{Vchannels: p.VChannels, Segments: p.Segments, Summary: p.Summary, Catalog: p.Catalog}}
	case CheckpointPublished:
		result.Payload = &streamingpb.WALReplicaEvent_CheckpointPublished{CheckpointPublished: p.Checkpoint}
	}
	return result
}

func EventFromProto(pb *streamingpb.WALReplicaEvent) (Event, error) {
	event := Event{Cursor: CursorFromProto(pb.GetCursor())}
	if event.Cursor.Term <= 0 || event.Cursor.Seq == 0 {
		return Event{}, merr.WrapErrServiceInternalMsg("invalid WAL replica event cursor")
	}
	switch p := pb.GetPayload().(type) {
	case *streamingpb.WALReplicaEvent_WalEntry:
		msg, err := decodeEntry(p.WalEntry)
		if err != nil {
			return Event{}, err
		}
		event.Payload = WALEntry{Message: msg}
	case *streamingpb.WALReplicaEvent_QueryViewIntents:
		event.Payload = QueryViewIntentBatch{Views: p.QueryViewIntents.GetViews()}
	case *streamingpb.WALReplicaEvent_StoragePublished:
		event.Payload = StoragePublished{VChannels: p.StoragePublished.GetVchannels(), Segments: p.StoragePublished.GetSegments(), Summary: p.StoragePublished.GetSummary(), Catalog: p.StoragePublished.GetCatalog()}
	case *streamingpb.WALReplicaEvent_CheckpointPublished:
		if _, err := message.UnmarshalMessageID(p.CheckpointPublished.GetMessageId()); err != nil {
			return Event{}, merr.Wrap(err, "decode published WAL checkpoint")
		}
		event.Payload = CheckpointPublished{Checkpoint: p.CheckpointPublished}
	default:
		return Event{}, merr.WrapErrServiceInternalMsg("missing WAL replica event payload")
	}
	event.Payload, _ = event.Payload.freeze()
	return event, nil
}

func decodeMessage(pb *commonpb.ImmutableMessage) (message.ImmutableMessage, error) {
	id, err := message.UnmarshalMessageID(pb.GetId())
	if err != nil {
		return nil, merr.Wrap(err, "decode WAL replica message ID")
	}
	// A decoded frame can be reused by its transport after Apply returns.
	owned := proto.CloneOf(pb)
	return message.NewImmutableMesasge(id, owned.Payload, owned.Properties), nil
}

func decodeEntry(entry *streamingpb.WALReplicaEntry) (message.ImmutableMessage, error) {
	outer, err := decodeMessage(entry.GetMessage())
	if err != nil {
		return nil, err
	}
	if outer.MessageType() != message.MessageTypeTxn {
		if entry.Begin != nil || entry.Commit != nil || len(entry.Body) != 0 {
			return nil, merr.WrapErrServiceInternalMsg("non-transaction WAL entry contains transaction fragments")
		}
		return outer, nil
	}
	begin, err := decodeMessage(entry.Begin)
	if err != nil {
		return nil, err
	}
	commit, err := decodeMessage(entry.Commit)
	if err != nil {
		return nil, err
	}
	b, err := message.AsImmutableBeginTxnMessageV2(begin)
	if err != nil {
		return nil, merr.Wrap(err, "decode WAL replica transaction begin")
	}
	c, err := message.AsImmutableCommitTxnMessageV2(commit)
	if err != nil {
		return nil, merr.Wrap(err, "decode WAL replica transaction commit")
	}
	if b.TxnContext() == nil || c.TxnContext() == nil || b.TxnContext().TxnID != c.TxnContext().TxnID || b.VChannel() != c.VChannel() || int(c.Header().GetBodyCnt()) != len(entry.Body) {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica transaction boundaries disagree")
	}
	builder := message.NewImmutableTxnMessageBuilder(b)
	for _, pb := range entry.Body {
		body, err := decodeMessage(pb)
		if err != nil {
			return nil, err
		}
		if body.TxnContext() == nil || body.TxnContext().TxnID != c.TxnContext().TxnID || body.VChannel() != c.VChannel() {
			return nil, merr.WrapErrServiceInternalMsg("WAL replica transaction body identity disagrees with commit")
		}
		builder.Add(body)
	}
	txn, err := builder.Build(c)
	if err != nil {
		return nil, merr.Wrap(err, "assemble WAL replica transaction")
	}
	if !proto.Equal(txn.IntoImmutableMessageProto(), entry.Message) {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica transaction envelope disagrees with commit")
	}
	return txn, nil
}
