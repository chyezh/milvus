package channel

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestChannelManager(t *testing.T) {
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))

	ctx := context.Background()
	// Test recover failure.
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, errors.New("recover failure"))
	m, err := RecoverChannelManager(ctx)
	assert.Nil(t, m)
	assert.Error(t, err)

	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelInfo, error) {
		return []*streamingpb.PChannelInfo{
			{
				Name:     "test-channel",
				Term:     1,
				ServerId: 1,
			},
		}, nil
	})
	m, err = RecoverChannelManager(ctx)
	assert.NotNil(t, m)
	assert.NoError(t, err)

	// Test save meta failure
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(errors.New("save meta failure"))
	pchannels, err := m.UpdatePChannelAndLoad(ctx, map[string]int64{"test-channel": 2})
	assert.Error(t, err)
	assert.Nil(t, pchannels)

	// Test update non exist pchannel
	pchannels, err = m.UpdatePChannelAndLoad(ctx, map[string]int64{"non-exist-channel": 2})
	assert.ErrorIs(t, err, ErrChannelNotExist)
	assert.Nil(t, pchannels)

	// Test success.
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	pchannels, err = m.UpdatePChannelAndLoad(ctx, map[string]int64{"test-channel": 2})
	assert.NoError(t, err)
	assert.NotNil(t, pchannels)
	assert.Equal(t, int64(2), pchannels["test-channel"].ServerID())
}
