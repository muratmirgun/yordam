//go:build acceptance

package acceptance_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const traceArchitecture = "PRD-FR-03; Capability 4.1-4.3; Foundation 12.1-12.3; Acceptance 6.4"

func acceptFoundationArchitecture(t *testing.T) {
	t.Logf("trace=%s", traceArchitecture)
	root := foundationRepositoryRoot(t)
	internalRoot := filepath.Join(root, "internal")
	fset := token.NewFileSet()

	journalCallAllowlist := map[string]bool{
		filepath.Join("internal", "orchestrator", "service.go"): true,
	}
	storageImplementationAllowlist := map[string]bool{
		filepath.Join("internal", "session", "jsonl"): true,
	}
	dispatchCallAllowlist := map[string]bool{
		filepath.Join("internal", "orchestrator", "service.go"):            true,
		filepath.Join("internal", "provider", "openaicompat", "router.go"): true,
		filepath.Join("internal", "tooling", "service.go"):                 true,
	}

	var appendCalls, storageCalls, dispatchCalls int
	err := filepath.WalkDir(internalRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if function.Name.Name == "AppendBatch" && function.Recv != nil && !underAllowedDirectory(rel, storageImplementationAllowlist) {
				t.Errorf("[%s] bounded service exposes journal append method at %s:%d", traceArchitecture, rel, fset.Position(function.Pos()).Line)
			}
		}
		if rel == filepath.Join("internal", "agent", "runner.go") {
			for _, declaration := range file.Decls {
				generic, ok := declaration.(*ast.GenDecl)
				if !ok || generic.Tok != token.TYPE {
					continue
				}
				for _, spec := range generic.Specs {
					if named, ok := spec.(*ast.TypeSpec); ok && named.Name.Name == "Runner" {
						t.Errorf("[%s] legacy monolithic agent.Runner still exists at %s:%d", traceArchitecture, rel, fset.Position(named.Pos()).Line)
					}
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver := selectorReceiver(selector.X)
			switch selector.Sel.Name {
			case "AppendBatch":
				appendCalls++
				if !journalCallAllowlist[rel] {
					t.Errorf("[%s] turn-journal AppendBatch call outside orchestrator at %s:%d", traceArchitecture, rel, fset.Position(call.Pos()).Line)
				}
			case "EncodeProposed":
				storageCalls++
				if !underAllowedDirectory(rel, storageImplementationAllowlist) {
					t.Errorf("[%s] journal storage encoding outside jsonl at %s:%d", traceArchitecture, rel, fset.Position(call.Pos()).Line)
				}
			case "Stream":
				if receiver == "l.scanner" || receiver == "l.redactor" || receiver == "owned" {
					return true
				}
				dispatchCalls++
				if !dispatchCallAllowlist[rel] {
					t.Errorf("[%s] provider Stream call outside dispatch services at %s:%d receiver=%s", traceArchitecture, rel, fset.Position(call.Pos()).Line, receiver)
				}
			case "Execute":
				if strings.HasSuffix(receiver, ".ApplicationService") {
					return true
				}
				dispatchCalls++
				if !dispatchCallAllowlist[rel] {
					t.Errorf("[%s] effect-capable Execute call outside dispatch services at %s:%d receiver=%s", traceArchitecture, rel, fset.Position(call.Pos()).Line, receiver)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("[%s] parse architecture: %v", traceArchitecture, err)
	}
	if appendCalls == 0 || storageCalls == 0 || dispatchCalls == 0 {
		t.Fatalf("[%s] architecture gate did not observe all boundaries: append=%d storage=%d dispatch=%d", traceArchitecture, appendCalls, storageCalls, dispatchCalls)
	}
	runFoundationGoTest(t, traceArchitecture, "./internal/app", `^TestRuntimeCompositionUsesOnlyOrchestratedRunner$`)
	runFoundationGoTest(t, traceArchitecture, "./internal/orchestrator", `^Test(DurableOrderingMutationPreviewCheckpointRevalidationExecutionAndContinuation|RunControlConsequentialEventCommitsInsideDispatchCallback|OperationLaneSerializesTurnsAndControlsAcrossSessions)$`)
	t.Logf("trace=%s parser_append_calls=%d parser_storage_calls=%d parser_dispatch_calls=%d status=pass", traceArchitecture, appendCalls, storageCalls, dispatchCalls)
}

func underAllowedDirectory(path string, allowlist map[string]bool) bool {
	for directory := range allowlist {
		if path == directory || strings.HasPrefix(path, directory+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func selectorReceiver(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		prefix := selectorReceiver(typed.X)
		if prefix == "" {
			return typed.Sel.Name
		}
		return prefix + "." + typed.Sel.Name
	case *ast.ParenExpr:
		return selectorReceiver(typed.X)
	default:
		return "<expression>"
	}
}
