package specaction

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/spec"
	"golang.org/x/tools/go/analysis"
)

var Analyzer = &analysis.Analyzer{
	Name: "specaction",
	Doc:  "ensures trace SpecAction values are valid spec actions",
	Run:  run,
}

var (
	verbose      bool
	verboseMode  string
	manifestPath string
)

func init() {
	Analyzer.Flags.BoolVar(&verbose, "verbose", false, "emit verbose analyzer output")
	Analyzer.Flags.BoolVar(&verbose, "specaction.verbose", false, "emit verbose analyzer output")
	Analyzer.Flags.StringVar(&verboseMode, "verbose-mode", "", "verbosity mode: all|checks|none")
	Analyzer.Flags.StringVar(&verboseMode, "specaction.verbose-mode", "", "verbosity mode: all|checks|none")
	Analyzer.Flags.StringVar(&manifestPath, "manifest", "", "override path to coverage manifest")
	Analyzer.Flags.StringVar(&manifestPath, "specaction.manifest", "", "override path to coverage manifest")
}

func run(pass *analysis.Pass) (interface{}, error) {
	manifests, manifestLocation, err := loadManifests()
	if err != nil {
		return nil, err
	}
	allowed := unionActions(manifests)
	checks := 0
	unknown := 0

	for _, file := range pass.Files {
		var stack []ast.Node
		var currentFunc *ast.FuncDecl
		ast.Inspect(file, func(node ast.Node) bool {
			if node == nil {
				if len(stack) == 0 {
					return false
				}
				popped := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if popped == currentFunc {
					currentFunc = nil
					for i := len(stack) - 1; i >= 0; i-- {
						if fn, ok := stack[i].(*ast.FuncDecl); ok {
							currentFunc = fn
							break
						}
					}
				}
				return false
			}

			stack = append(stack, node)
			if fn, ok := node.(*ast.FuncDecl); ok {
				currentFunc = fn
			}

			switch n := node.(type) {
			case *ast.CallExpr:
				checks += checkEmitTrace(pass, n, allowed, &unknown)
			case *ast.CompositeLit:
				checks += checkTraceEventLiteral(pass, n, allowed, currentFunc, &unknown)
			}
			return true
		})
	}

	mode := strings.TrimSpace(verboseMode)
	if mode == "" {
		if verbose {
			mode = "all"
		} else {
			mode = "checks"
		}
	}
	switch mode {
	case "none":
		return nil, nil
	case "checks":
		if checks == 0 && unknown == 0 {
			return nil, nil
		}
	case "all":
		// fallthrough to print
	default:
		if verbose || checks > 0 || unknown > 0 {
			fmt.Fprintf(os.Stderr, "specaction: unknown verbose-mode=%q (use all|checks|none)\n", mode)
		}
		return nil, nil
	}

	fmt.Fprintf(os.Stderr, "specaction: pkg=%s manifest=%s actions=%d checks=%d unknown=%d\n", pass.Pkg.Path(), manifestLocation, len(allowed), checks, unknown)

	return nil, nil
}

func checkEmitTrace(pass *analysis.Pass, call *ast.CallExpr, allowed map[spec.Action]struct{}, unknown *int) int {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel == nil || selector.Sel.Name != "emitTrace" {
		return 0
	}
	selection, ok := pass.TypesInfo.Selections[selector]
	if !ok || selection.Obj() == nil || selection.Obj().Pkg() == nil {
		return 0
	}
	if selection.Obj().Pkg().Path() != "github.com/josephjohncox/wallaby/pkg/stream" {
		return 0
	}
	if len(call.Args) < 5 {
		return 0
	}
	specVal, specOK := checkSpecActionExpr(pass, call.Args[4], allowed, unknown)
	if kindVal, kindOK := constString(pass.TypesInfo, call.Args[1]); kindOK && specOK {
		checkKindSpecActionMapping(pass, call.Args[4].Pos(), kindVal, specVal, unknown)
	}
	return 1
}

func checkTraceEventLiteral(pass *analysis.Pass, lit *ast.CompositeLit, allowed map[spec.Action]struct{}, currentFunc *ast.FuncDecl, unknown *int) int {
	if !isTraceEventType(pass.TypesInfo.TypeOf(lit.Type)) {
		return 0
	}
	if isRunnerEmitTrace(currentFunc, pass.TypesInfo) {
		return 0
	}
	checked := 0
	var kindVal string
	var kindOK bool
	var specVal string
	var specOK bool
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "SpecAction" {
			if ok && key.Name == "Kind" {
				kindVal, kindOK = constString(pass.TypesInfo, kv.Value)
			}
			continue
		}
		specVal, specOK = checkSpecActionExpr(pass, kv.Value, allowed, unknown)
		checked++
	}
	if kindOK && specOK {
		checkKindSpecActionMapping(pass, lit.Pos(), kindVal, specVal, unknown)
	}
	return checked
}

func checkSpecActionExpr(pass *analysis.Pass, expr ast.Expr, allowed map[spec.Action]struct{}, unknown *int) (string, bool) {
	value, ok := constString(pass.TypesInfo, expr)
	if !ok {
		pass.Reportf(expr.Pos(), "SpecAction must be a constant spec.Action")
		*unknown++
		return "", false
	}
	if value == "" {
		return value, true
	}
	action := spec.Action(value)
	if _, ok := allowed[action]; !ok {
		pass.Reportf(expr.Pos(), "SpecAction %q not listed in spec manifest", value)
		*unknown++
	}
	return value, true
}

func constString(info *types.Info, expr ast.Expr) (string, bool) {
	tv, ok := info.Types[expr]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

func isTraceEventType(t types.Type) bool {
	switch tt := t.(type) {
	case *types.Named:
		return isTraceEventNamed(tt)
	case *types.Pointer:
		if named, ok := tt.Elem().(*types.Named); ok {
			return isTraceEventNamed(named)
		}
	}
	return false
}

func isTraceEventNamed(named *types.Named) bool {
	obj := named.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	return obj.Pkg().Path() == "github.com/josephjohncox/wallaby/pkg/stream" && obj.Name() == "TraceEvent"
}

func isRunnerEmitTrace(fn *ast.FuncDecl, info *types.Info) bool {
	if fn == nil || fn.Name == nil || fn.Name.Name != "emitTrace" {
		return false
	}
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}
	recvType := info.TypeOf(fn.Recv.List[0].Type)
	ptr, ok := recvType.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "github.com/josephjohncox/wallaby/pkg/stream" && named.Obj().Name() == "Runner"
}

func loadManifests() (map[spec.SpecName]spec.Manifest, string, error) {
	if manifestPath != "" {
		manifests, err := spec.LoadManifests(manifestPath)
		return manifests, manifestPath, err
	}
	root, err := findModuleRoot()
	if err != nil {
		return nil, "", err
	}
	path := filepath.Join(root, "specs")
	manifests, err := spec.LoadManifests(path)
	return manifests, path, err
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", fmt.Errorf("go.mod not found for spec manifest lookup")
}

func unionActions(manifests map[spec.SpecName]spec.Manifest) map[spec.Action]struct{} {
	allowed := make(map[spec.Action]struct{})
	for _, manifest := range manifests {
		for _, action := range manifest.Actions {
			allowed[action] = struct{}{}
		}
	}
	return allowed
}

func checkKindSpecActionMapping(pass *analysis.Pass, pos token.Pos, kind string, specValue string, unknown *int) {
	allowed, ok := kindSpecActionMap[kind]
	if !ok {
		return
	}
	action := spec.Action(specValue)
	if _, ok := allowed[action]; ok {
		return
	}
	pass.Reportf(pos, "SpecAction %q does not match Kind %q", specValue, kind)
	*unknown++
}

var kindSpecActionMap = map[string]map[spec.Action]struct{}{
	"read": {
		spec.ActionReadBatch: {},
		spec.ActionReadDDL:   {},
	},
	"read_error": {
		spec.ActionReadFail: {},
	},
	"deliver": {
		spec.ActionDeliver: {},
	},
	"write_error": {
		spec.ActionWriteFail: {},
	},
	"ack": {
		spec.ActionAck: {},
	},
	"ddl_applied": {
		spec.ActionApplyDDL: {},
	},
	"ddl_approved": {
		spec.ActionApproveDDL: {},
	},
	"write": {
		spec.ActionNone: {},
	},
	"ack_error": {
		spec.ActionNone: {},
	},
	"checkpoint": {
		spec.ActionNone: {},
	},
	"control_checkpoint": {
		spec.ActionNone: {},
	},
	"ddl_error": {
		spec.ActionNone: {},
	},
	"flow_state": {
		spec.ActionStart:   {},
		spec.ActionPause:   {},
		spec.ActionResume:  {},
		spec.ActionStop:    {},
		spec.ActionFail:    {},
		spec.ActionRunOnce: {},
	},
}
