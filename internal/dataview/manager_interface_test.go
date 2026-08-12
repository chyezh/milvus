// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataview

import (
	"go/build"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataViewCoreDoesNotDependOnBalancer(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	pkg, err := build.Default.ImportDir(filepath.Dir(filename), 0)
	require.NoError(t, err)
	for _, imported := range pkg.Imports {
		require.False(t, strings.HasPrefix(imported, "github.com/milvus-io/milvus/internal/views/coord/balancer"),
			"DataView core must not depend on Balancer consumer package %s", imported)
	}
}

func TestManagerExposesStateOperationsInsteadOfLifecycleEvents(t *testing.T) {
	managerType := reflect.TypeOf((*Manager)(nil)).Elem()

	for _, method := range []string{"InitializeCollection", "MarkCollectionTerminal"} {
		_, ok := managerType.MethodByName(method)
		require.True(t, ok, "dataview.Manager must expose state operation %s", method)
	}
	for _, method := range []string{"OnCreateCollection", "OnDropCollection"} {
		_, ok := managerType.MethodByName(method)
		require.False(t, ok, "dataview.Manager must not expose lifecycle event method %s", method)
	}
}
