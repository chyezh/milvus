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

package datacoord

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitMetaRecoversDataViewAfterSegmentMetaIsReady(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	serverFile := filepath.Join(filepath.Dir(currentFile), "server.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), serverFile, nil, 0)
	require.NoError(t, err)

	var initMeta *ast.FuncDecl
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "initMeta" {
			initMeta = fn
			break
		}
	}
	require.NotNil(t, initMeta)

	newMetaPosition := token.NoPos
	recoverPosition := token.NoPos
	ast.Inspect(initMeta.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch functionName(call.Fun) {
		case "newMeta":
			newMetaPosition = call.Pos()
		case "RecoverManager":
			recoverPosition = call.Pos()
		}
		return true
	})

	require.NotEqual(t, token.NoPos, newMetaPosition)
	require.NotEqual(t, token.NoPos, recoverPosition)
	require.Less(t, newMetaPosition, recoverPosition,
		"DataView recovery may read SegmentMeta and must run after newMeta completes")
}

func functionName(expr ast.Expr) string {
	switch function := expr.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		return function.Sel.Name
	default:
		return ""
	}
}
