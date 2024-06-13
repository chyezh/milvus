package timetick

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mocks/lognode/server/wal/mock_walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/stretchr/testify/mock"
)

func TestTimeTickAppendInterceptor(t *testing.T) {
	rootCoordClient := timestamp.NewMockRootCoordClient(t)
	b := NewInterceptorBuilder(rootCoordClient)
	wal := mock_walimpls.NewMockWALImpls(t)
	msgs := make([]message.ImmutableMessage, 0)
	id := int64(1)
	wal.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		id++
		msgID := message.NewTestMessageID(id)
		msgs = append(msgs, mm.IntoImmutableMessage(msgID))
		return msgID, nil
	})
	ch := make(chan walimpls.WALImpls, 1)
	ch <- wal

	interceptor := b.Build(ch)
}
