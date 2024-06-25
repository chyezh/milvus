package assignment

import (
	"context"
)

const (
	minimalTerm = int64(-1)
)

var _ Watcher = (*watcherImpl)(nil)

// Assignment is the channel assignment.
type Assignment struct {
	PChannel string
	Term     int64
	ServerID int64
}

// Watcher is the interface for the channel assignment.
type Watcher interface {
	// Get returns the channel assignment.
	Get(ctx context.Context, pchannel string) *Assignment

	// Watch watches the channel assignment.
	// Block until new term is coming.
	Watch(ctx context.Context, channel string, oldAssign *Assignment) error

	// Close stop the watcher.
	Close()
}
