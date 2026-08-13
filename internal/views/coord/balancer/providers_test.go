package balancer

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
)

var _ DataViewProvider = (dataview.Manager)(nil)

func TestDataViewInterfacesExposeOnlyReferencedSnapshots(t *testing.T) {
	managerType := reflect.TypeOf((*dataview.Manager)(nil)).Elem()
	providerType := reflect.TypeOf((*DataViewProvider)(nil)).Elem()

	for _, method := range []string{"Snapshot", "DataViewSnapshot", "DataViewSnapshotForCollections"} {
		_, exposedByManager := managerType.MethodByName(method)
		require.False(t, exposedByManager, "dataview.Manager must not expose bare snapshot method %s", method)
		_, exposedByProvider := providerType.MethodByName(method)
		require.False(t, exposedByProvider, "DataViewProvider must not expose bare snapshot method %s", method)
	}

	_, managerHasRef := managerType.MethodByName("DataViewSnapshotRefForCollections")
	require.True(t, managerHasRef)
	_, providerHasRef := providerType.MethodByName("DataViewSnapshotRefForCollections")
	require.True(t, providerHasRef)
}
