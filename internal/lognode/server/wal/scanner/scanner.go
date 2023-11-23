package scanner

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

var _ Scanner = (*mqBasedScanner)(nil) // _ scannerCreator = (*mqBasedScannerCreator)(nil)

// NewMQBasedManagement creates a new scanner management based on message queue.
func NewMQBasedManagement(c mqwrapper.Client, channel logpb.PChannelInfo) Manager {
	return &mqBasedScannerManagement{
		c: c,
	}
}

// AllocateParam is the parameter for creating scanner.
type AllocateParam struct {
	Channel       logpb.PChannelInfo
	ScannerName   string // A global unique id for a scanner.(such as mq.subscriptionName)
	DeliverPolicy options.DeliverPolicy
}

type Manager interface {
	// Allocate a scanner for reading records from the wal.
	Allocate(ctx context.Context, param AllocateParam) (Scanner, error)

	// Drop a scanner with scanner name.
	Drop(scannerName string) (err error)
}

// Scanner is the interface for reading records from the wal.
type Scanner interface {
	// Channel returns the channel assignment info of the wal.
	Channel() logpb.PChannelInfo

	// Chan returns the channel of message.
	Chan() <-chan message.ImmutableMessage

	// Error returns the error of scanner failed.
	// Will block until scanner is closed or Chan is dry out.
	Error() error

	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Close the scanner, release the underlying resources.
	// Return the error same with `Error`
	Close() error
}

type LastMessageIDQuerier interface {
	// GetLatestMessageID returns the latest message id of the channel.
	// GetLatestMessageID should be a method of wal, but not scanner.
	// But underlying mq based consumer implement it, do not on client.
	// So we put it here as a checker for mq based scanner.
	GetLatestMessageID(ctx context.Context) (message.MessageID, error)
}
