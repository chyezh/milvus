package resolver

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"google.golang.org/grpc/resolver"
)

type VersionedState = discoverer.VersionedState

// Builder is the interface for the grpc resolver builder.
// It owns a Resolver instance and build grpc.Resolver from it.
type Builder interface {
	resolver.Builder

	// Resolver returns the underlying resolver instance.
	Resolver() Resolver

	// Close the builder, release the underlying resolver instance.
	Close()
}

// Resolver is the interface for the service discovery in grpc.
// Allow the user to get the grpc service discovery results and watch the changes.
// Not all changes can be arrived by these api, only the newest state is guaranteed.
type Resolver interface {
	// GetLatestState returns the latest state of the resolver.
	// The returned state should be read only, applied any change to it will cause data race.
	GetLatestState() VersionedState

	// Watch watch the state change of the resolver.
	// cb will be called with latest state after call, and will be called with new state when state changed.
	// version may be skipped if the state is changed too fast.
	// last version can be seen by cb.
	// ctx.Cancel() should be called to release the underlying resource.
	Watch(ctx context.Context, cb func(VersionedState))
}
