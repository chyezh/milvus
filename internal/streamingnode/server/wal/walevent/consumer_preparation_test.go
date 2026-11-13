package walevent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConsumerPreparesDependenciesBeforeChangingPrefix(t *testing.T) {
	prepared, applied := 0, 0
	var preparationError, applyError error
	consumer := NewConsumer(Cursor{Term: 7}, func(context.Context, Event) error {
		applied++
		return applyError
	}, WithPreparation(func(context.Context, Event) error {
		prepared++
		return preparationError
	}))
	event := Event{Cursor: Cursor{Term: 7, Seq: 1}}
	preparationError = NewBaselineRequired(consumer.Cursor(), merr.ErrServiceUnavailable)
	require.ErrorIs(t, consumer.Apply(t.Context(), event), merr.ErrServiceUnavailable)
	require.NoError(t, consumer.Err())
	require.Equal(t, 0, applied)
	require.EqualValues(t, 0, consumer.Cursor().Seq)
	preparationError = nil
	require.NoError(t, consumer.Apply(t.Context(), event))
	require.Equal(t, 2, prepared)
	require.Equal(t, 1, applied)
	require.NoError(t, consumer.Apply(t.Context(), event))
	require.Error(t, consumer.Apply(t.Context(), Event{Cursor: Cursor{Term: 7, Seq: 3}}))
	require.Error(t, consumer.Apply(t.Context(), Event{Cursor: Cursor{Term: 8, Seq: 2}}))
	require.Equal(t, 2, prepared, "duplicate, gap and wrong-term events must not acquire dependencies")
	event.Cursor.Seq = 2
	applyError = merr.ErrDataIntegrity
	require.ErrorIs(t, consumer.Apply(t.Context(), event), merr.ErrDataIntegrity)
	require.ErrorIs(t, consumer.Err(), merr.ErrDataIntegrity)
	require.EqualValues(t, 1, consumer.Cursor().Seq)
	require.ErrorIs(t, consumer.Apply(t.Context(), event), merr.ErrDataIntegrity)
	require.Equal(t, 3, prepared, "a partial Apply is terminal, even if acquisition succeeded")
}
