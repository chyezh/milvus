package walevent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPublishedMetadataIsFrozen(t *testing.T) {
	ctx := testContext(t)
	s := NewSource(7, 8, 1<<20, func(context.Context, Event) error { return nil })
	defer s.Close()
	r, err := s.Subscribe(Cursor{Term: 7})
	require.NoError(t, err)
	defer r.Close()
	views := QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{{Meta: &viewpb.QueryViewMeta{ReplicaId: 10}}}}
	storage := StoragePublished{
		VChannels: []*streamingpb.VChannelMeta{{Vchannel: "v1", TransformMaterializedTimeTick: 12}},
		Segments:  []*streamingpb.SegmentAssignmentMeta{{SegmentId: 20, PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: "immutable-object"}}},
		Summary:   &streamingpb.PChannelSummaryManifest{Coverage: &streamingpb.SummaryCoverage{Generation: 30}},
	}
	cp := CheckpointPublished{Checkpoint: &streamingpb.WALCheckpoint{TimeTick: 40}}
	msg := message.NewImmutableMesasge(nil, []byte("payload"), map[string]string{})
	owner := message.NewOwnedImmutableMessage(msg, nil)
	for _, payload := range []Payload{views, storage, cp, WALEntry{Message: owner.Message()}, StoragePublished{}} {
		_, err := s.Publish(ctx, payload)
		require.NoError(t, err)
	}
	views.Views[0].Meta.ReplicaId++
	storage.Segments[0].PersistedStorage.ManifestPath = "changed"
	storage.VChannels[0].TransformMaterializedTimeTick++
	storage.Summary.Coverage.Generation++
	cp.Checkpoint.TimeTick++
	// Journal ownership must not depend on the persistence ACK lifetime.
	owner.Release()
	e, err := r.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 10, e.Payload.(QueryViewIntentBatch).Views[0].Meta.ReplicaId)
	e, err = r.Next(ctx)
	require.NoError(t, err)
	published := e.Payload.(StoragePublished)
	require.EqualValues(t, 12, published.VChannels[0].TransformMaterializedTimeTick)
	require.Equal(t, "immutable-object", published.Segments[0].PersistedStorage.ManifestPath)
	require.EqualValues(t, 30, published.Summary.Coverage.Generation)
	e, err = r.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 40, e.Payload.(CheckpointPublished).Checkpoint.TimeTick)
	e, err = r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), e.Payload.(WALEntry).Message.Payload())
	e, err = r.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, e.Payload.(StoragePublished).Summary)
}
