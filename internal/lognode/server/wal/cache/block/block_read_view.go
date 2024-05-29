package block

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// NewBlockReadViewScanner creates a new block scanner that scans messages in a block.
func NewBlockReadViewScanner(scanners []BlockScanner) BlockScanner {
	return &blockReadViewScanner{
		scanner: scanners,
	}
}

// blockReadViewScanner is a consintuous series of scanner that can be read.
type blockReadViewScanner struct {
	scanner []BlockScanner
}

// Scan scans the next message.
func (b *blockReadViewScanner) Scan(ctx context.Context) error {
	for len(b.scanner) > 0 {
		err := b.scanner[0].Scan(ctx)
		if err == io.EOF {
			b.scanner = b.scanner[1:]
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}
	return io.EOF
}

// Message returns the current message.
func (b *blockReadViewScanner) Message() message.ImmutableMessage {
	return b.scanner[0].Message()
}
