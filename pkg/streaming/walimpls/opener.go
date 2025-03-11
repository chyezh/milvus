package walimpls

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// OpenOption is the option for allocating wal impls instance.
type OpenOption struct {
	Channel    types.PChannelInfo // Channel to open.
	AccessMode types.AccessMode   // Access mode, if AccessModeRO, the wal impls should be read-only, the append operation will panics.
	// If accessMode is AccessModeRW, the wal impls should be read-write,
	// and it will fence the old rw wal impls or wait the old rw wal impls close.
}

// Validate validates the OpenOption.
func (oo OpenOption) Validate() error {
	if oo.Channel.Name == "" {
		return errors.New("channel name is empty")
	}
	if oo.Channel.Term < 0 {
		return errors.New("channel term is negative")
	}
	if oo.AccessMode != types.AccessModeRO && oo.AccessMode != types.AccessModeRW {
		return errors.New("undefined access mode")
	}
	return nil
}

// OpenerImpls is the interface for build WALImpls instance.
type OpenerImpls interface {
	// Open open a WALImpls instance.
	// If the opt.AccessMode is AccessModeRO, the WALImpls should be read-only, the append operation will panic.
	Open(ctx context.Context, opt *OpenOption) (WALImpls, error)

	// Close release the resources.
	Close()
}
