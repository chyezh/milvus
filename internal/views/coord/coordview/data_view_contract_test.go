package coordview

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRecoverShardViewRegistryRequiresDataViewManager(t *testing.T) {
	constructor := reflect.TypeOf(RecoverShardViewRegistry)

	require.False(t, constructor.IsVariadic(), "DataView reference ownership must be a required constructor dependency")
	require.Equal(t, 4, constructor.NumIn())
	require.Equal(t, reflect.TypeOf((*DataViewManager)(nil)).Elem(), constructor.In(3))
}

func TestRecoverShardViewRegistryRejectsNilDataViewManager(t *testing.T) {
	registry, err := RecoverShardViewRegistry(context.Background(), newMockCatalog(), newMockSyncer(), nil)

	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, registry)
}
