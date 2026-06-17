package viewresource

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestSchedulerSubmitAfterCloseCompletesTask(t *testing.T) {
	scheduler := NewScheduler(1)
	scheduler.Close()

	task := newResourceBuildTask(context.Background(), BuildKey{
		CollectionID: 1,
		VChannel:     "ch",
		DataVersion:  qviews.DataVersion{StreamingVersion: 1},
	}, func(context.Context) (*ViewRuntime, error) {
		return nil, errors.New("unexpected build")
	})

	scheduler.Submit(task)
	select {
	case <-task.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled task")
	}
	runtime, err := task.Result()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, runtime)
}

func TestSchedulerCloseCompletesQueuedTask(t *testing.T) {
	scheduler := NewScheduler(1)
	runningStarted := make(chan struct{})
	releaseRunning := make(chan struct{})
	running := newResourceBuildTask(context.Background(), BuildKey{
		CollectionID: 1,
		VChannel:     "ch",
		DataVersion:  qviews.DataVersion{StreamingVersion: 1},
	}, func(context.Context) (*ViewRuntime, error) {
		close(runningStarted)
		<-releaseRunning
		return &ViewRuntime{}, nil
	})
	queued := newResourceBuildTask(context.Background(), BuildKey{
		CollectionID: 1,
		VChannel:     "ch",
		DataVersion:  qviews.DataVersion{StreamingVersion: 2},
	}, func(context.Context) (*ViewRuntime, error) {
		return nil, errors.New("unexpected queued build")
	})

	scheduler.Submit(running)
	select {
	case <-runningStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for running task")
	}
	scheduler.Submit(queued)

	closed := make(chan struct{})
	go func() {
		scheduler.Close()
		close(closed)
	}()

	select {
	case <-queued.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued task cancellation")
	}
	runtime, err := queued.Result()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, runtime)

	close(releaseRunning)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduler close")
	}
}
