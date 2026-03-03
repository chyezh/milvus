// ctxcheck scans Go packages and finds log calls where context.TODO() is used
// but a context.Context variable is actually available in scope.
//
// Usage:
//
//	go run scripts/ctxcheck/main.go [-fix] [-tags "dynamic,test"] ./internal/... ./pkg/...
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// logMethods are the method/function names on log.Logger or package log that take ctx as first arg.
var logMethods = map[string]bool{
	"Info": true, "Warn": true, "Error": true, "Debug": true, "Log": true,
	"RatedInfo": true, "RatedWarn": true, "RatedError": true, "RatedDebug": true,
}

var (
	doFix    = flag.Bool("fix", false, "auto-fix: rewrite files in place")
	tagsList = flag.String("tags", "dynamic", "build tags (comma-separated)")
)

type finding struct {
	pos     token.Position
	ctxExpr string // e.g. "ctx", "s.ctx"
}

func main() {
	flag.Parse()
	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	var buildFlags []string
	if *tagsList != "" {
		buildFlags = append(buildFlags, "-tags="+*tagsList)
	}

	cfg := &packages.Config{
		Mode: packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo |
			packages.NeedName | packages.NeedFiles,
		BuildFlags: buildFlags,
	}

	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: %v\n", err)
		os.Exit(1)
	}

	// Collect all findings, grouped by file for fixing
	type fileFix struct {
		file     *ast.File
		fset     *token.FileSet
		findings []finding
		// AST nodes to rewrite (the context.TODO() CallExpr)
		nodes []*ast.CallExpr
		// replacement idents
		replacements []ast.Expr
	}
	fileFixMap := map[string]*fileFix{}

	total := 0
	fixable := 0

	for _, pkg := range pkgs {
		if pkg.TypesInfo == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			fname := pkg.Fset.Position(file.Pos()).Filename
			// Skip pkg/log itself
			if strings.Contains(fname, "/pkg/log/") && !strings.Contains(fname, "/pkg/log/logcore/") {
				continue
			}

			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}

				if !isLogMethodCall(call, pkg.TypesInfo) {
					return true
				}

				if !isContextTODO(call.Args[0]) {
					return true
				}

				total++
				pos := pkg.Fset.Position(call.Args[0].Pos())

				// Find context.Context variables in scope
				ctxExpr := findAvailableCtx(call.Pos(), file, pkg)
				if ctxExpr == "" {
					return true
				}

				fixable++
				f := finding{pos: pos, ctxExpr: ctxExpr}
				fmt.Printf("%s:%d: context.TODO() -> %s\n", pos.Filename, pos.Line, ctxExpr)

				if *doFix {
					ff, ok := fileFixMap[fname]
					if !ok {
						ff = &fileFix{file: file, fset: pkg.Fset}
						fileFixMap[fname] = ff
					}
					ff.findings = append(ff.findings, f)
					ff.nodes = append(ff.nodes, call)
					ff.replacements = append(ff.replacements, makeCtxExpr(ctxExpr))
				}

				return true
			})
		}
	}

	fmt.Fprintf(os.Stderr, "\nTotal context.TODO() in log calls: %d\n", total)
	fmt.Fprintf(os.Stderr, "Fixable (ctx in scope): %d\n", fixable)
	fmt.Fprintf(os.Stderr, "Not fixable (no ctx): %d\n", total-fixable)

	if *doFix && len(fileFixMap) > 0 {
		fixed := 0
		for fname, ff := range fileFixMap {
			for i, call := range ff.nodes {
				call.Args[0] = ff.replacements[i]
			}
			var buf bytes.Buffer
			if err := format.Node(&buf, ff.fset, ff.file); err != nil {
				fmt.Fprintf(os.Stderr, "format %s: %v\n", fname, err)
				continue
			}
			if err := os.WriteFile(fname, buf.Bytes(), 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "write %s: %v\n", fname, err)
				continue
			}
			fixed++
		}
		fmt.Fprintf(os.Stderr, "Fixed %d files\n", fixed)
	}
}

// isLogMethodCall checks if the call is a log method (Info/Warn/etc) from our log package.
func isLogMethodCall(call *ast.CallExpr, info *types.Info) bool {
	var methodName string

	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		methodName = fn.Sel.Name
	default:
		return false
	}

	if !logMethods[methodName] {
		return false
	}

	// Check that the first arg position is context-like (heuristic: has at least 2 args)
	if len(call.Args) < 2 {
		return false
	}

	// Use type info to verify the function signature expects context.Context as first param
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	// Get the type of the method/function
	obj := info.ObjectOf(sel.Sel)
	if obj == nil {
		return false
	}

	sig, ok := obj.Type().(*types.Signature)
	if !ok {
		return false
	}

	params := sig.Params()
	if params.Len() == 0 {
		return false
	}

	// Check if first param is context.Context
	firstParam := params.At(0)
	return isContextType(firstParam.Type())
}

func isContextType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Name() == "Context" && obj.Pkg() != nil && obj.Pkg().Path() == "context"
}

// isContextTODO checks if an expression is context.TODO()
func isContextTODO(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == "context" && sel.Sel.Name == "TODO"
}

// findAvailableCtx looks for a context.Context variable available at the given position.
// It checks: function params, local variables, receiver struct fields.
func findAvailableCtx(pos token.Pos, file *ast.File, pkg *packages.Package) string {
	info := pkg.TypesInfo

	// Use go/types scope to find the innermost scope at this position
	innerScope := info.Scopes[file].Innermost(pos)
	if innerScope == nil {
		return ""
	}

	// Walk up scopes looking for context.Context variables
	for scope := innerScope; scope != nil; scope = scope.Parent() {
		names := scope.Names()
		for _, name := range names {
			obj := scope.Lookup(name)
			if obj == nil {
				continue
			}
			// Skip variables defined AFTER our position
			if obj.Pos() > pos {
				continue
			}
			if isContextType(obj.Type()) {
				return name
			}
		}
		// Stop at function scope (don't go to package scope)
		if scope.Parent() != nil && scope.Parent().Parent() == nil {
			break
		}
	}

	// Check receiver struct fields
	// Find the enclosing FuncDecl
	var enclosingFunc *ast.FuncDecl
	ast.Inspect(file, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}
		if fd.Body != nil && fd.Body.Pos() <= pos && pos <= fd.Body.End() {
			enclosingFunc = fd
		}
		return true
	})

	if enclosingFunc != nil && enclosingFunc.Recv != nil && len(enclosingFunc.Recv.List) > 0 {
		recv := enclosingFunc.Recv.List[0]
		if len(recv.Names) > 0 {
			recvName := recv.Names[0].Name
			recvObj := info.ObjectOf(recv.Names[0])
			if recvObj != nil {
				recvType := recvObj.Type()
				// Dereference pointer
				if ptr, ok := recvType.(*types.Pointer); ok {
					recvType = ptr.Elem()
				}
				if named, ok := recvType.(*types.Named); ok {
					if st, ok := named.Underlying().(*types.Struct); ok {
						for i := 0; i < st.NumFields(); i++ {
							field := st.Field(i)
							if isContextType(field.Type()) {
								return recvName + "." + field.Name()
							}
						}
					}
				}
			}
		}
	}

	return ""
}

func makeCtxExpr(ctxExpr string) ast.Expr {
	parts := strings.Split(ctxExpr, ".")
	if len(parts) == 1 {
		return ast.NewIdent(parts[0])
	}
	// a.b -> SelectorExpr
	return &ast.SelectorExpr{
		X:   ast.NewIdent(parts[0]),
		Sel: ast.NewIdent(parts[1]),
	}
}

// For sorting findings by position
type byPosition []finding

func (a byPosition) Len() int           { return len(a) }
func (a byPosition) Less(i, j int) bool { return a[i].pos.Offset < a[j].pos.Offset }
func (a byPosition) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

var _ sort.Interface = byPosition{}
