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
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
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

func TestDataViewDependenciesRequireDurablePublicationAndFlushVersionState(t *testing.T) {
	catalogType := reflect.TypeOf((*Catalog)(nil)).Elem()
	for _, method := range []string{
		"GetDataViewVersionState",
		"SaveDataViewVersionState",
		"SavePublishedDataView",
	} {
		_, ok := catalogType.MethodByName(method)
		require.True(t, ok, "dataview.Catalog must require %s", method)
	}
	_, exposesBareSnapshotWrite := catalogType.MethodByName("SaveDataView")
	require.False(t, exposesBareSnapshotWrite, "dataview.Catalog must not expose non-atomic snapshot publication")

	segmentStoreType := reflect.TypeOf((*SegmentStore)(nil)).Elem()
	for _, method := range []string{
		"ListAllSegmentsForVersionAllocation",
		"SaveSealedAtDataVersion",
	} {
		_, ok := segmentStoreType.MethodByName(method)
		require.True(t, ok, "dataview.SegmentStore must require %s", method)
	}
	_, exposesMembershipScan := segmentStoreType.MethodByName("SelectSegments")
	require.False(t, exposesMembershipScan, "dataview.SegmentStore must not expose collection membership scans")
}

func TestDataViewSegmentProjectionExcludesMembershipInferenceFields(t *testing.T) {
	segmentType := reflect.TypeOf(Segment{})
	for _, field := range []string{"InsertChannel", "DmlPosition", "CreatedByCompaction", "CompactionFrom"} {
		_, ok := segmentType.FieldByName(field)
		require.False(t, ok, "dataview.Segment must not carry legacy membership inference field %s", field)
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

func TestDataViewManagerResponsibilitiesStayInFocusedFiles(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	packageDir := filepath.Dir(filename)
	parsed, err := parser.ParseDir(token.NewFileSet(), packageDir, nil, 0)
	require.NoError(t, err)

	functionFiles := make(map[string]string)
	typeFiles := make(map[string]string)
	for sourceFile, file := range parsed["dataview"].Files {
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				name := declaration.Name.Name
				if declaration.Recv != nil {
					receiver := declaration.Recv.List[0].Type
					if pointer, ok := receiver.(*ast.StarExpr); ok {
						receiver = pointer.X
					}
					if identifier, ok := receiver.(*ast.Ident); ok {
						name = identifier.Name + "." + name
					}
				}
				functionFiles[name] = filepath.Base(sourceFile)
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if ok {
						typeFiles[typeSpec.Name.Name] = filepath.Base(sourceFile)
					}
				}
			}
		}
	}

	expectedTypeFiles := map[string]string{
		"SegmentStore":             "segment.go",
		"Segment":                  "segment.go",
		"dataViewUnavailableError": "access.go",
	}
	for declaration, expectedFile := range expectedTypeFiles {
		require.Equal(t, expectedFile, typeFiles[declaration],
			"%s must stay in the focused DataView unit %s", declaration, expectedFile)
	}

	expectedFunctionFiles := map[string]string{
		"RecoverManager": "recovery.go",
		"dataViewManager.latestLegacyLoadablePersistedView": "recovery.go",
		"dataViewManager.InitializeCollection":              "collection.go",
		"dataViewManager.MarkCollectionTerminal":            "collection.go",
		"dataViewManager.FinalizeDropCollection":            "collection.go",
		"dataViewManager.Get":                               "access.go",
		"dataViewManager.LatestPublished":                   "access.go",
		"dataViewManager.SegmentSnapshot":                   "frontier.go",
		"dataViewManager.ShardTimeTicks":                    "frontier.go",
		"segmentTransformStartAfterTimetick":                "frontier.go",
		"dataViewManager.IsSegmentReferenced":               "gc.go",
		"dataViewManager.GarbageCollect":                    "gc.go",
		"dataViewManager.updateRetainedMembership":          "gc.go",
		"canonicalDataViewClone":                            "view.go",
		"cloneDataVersion":                                  "view.go",
		"dataVersionKey":                                    "view.go",
	}
	for declaration, expectedFile := range expectedFunctionFiles {
		require.Equal(t, expectedFile, functionFiles[declaration],
			"%s must stay in the focused DataView unit %s", declaration, expectedFile)
	}
}
