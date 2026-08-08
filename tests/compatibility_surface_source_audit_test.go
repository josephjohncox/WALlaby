package tests

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFirstPartyCapabilityMatrixAndConfiguredVariantsAreAudited(t *testing.T) {
	registrations := runner.DestinationRegistrations()
	contracts, err := runner.DestinationContracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != len(contracts) {
		t.Fatalf("registry rows=%d matrix rows=%d", len(registrations), len(contracts))
	}
	seen := make(map[connector.EndpointType]bool, len(registrations))
	for _, registration := range registrations {
		if seen[registration.Type] {
			t.Fatalf("duplicate destination registry row %s", registration.Type)
		}
		seen[registration.Type] = true
		var destination connector.Destination
		if registration.New != nil {
			destination = registration.New()
		}
		_, configurationAware := destination.(connector.ConfiguredDestinationCapabilities)
		if configurationAware != (len(registration.Profiles) > 0) {
			t.Errorf("%s configuration-aware resolver=%t profiles=%d", registration.Type, configurationAware, len(registration.Profiles))
		}
		if configurationAware {
			configured := destination.(connector.ConfiguredDestinationCapabilities)
			declared := make(map[connector.CapabilityProfileID]struct{})
			for _, profileID := range configured.CapabilityProfileIDs() {
				declared[profileID] = struct{}{}
			}
			registered := make(map[connector.CapabilityProfileID]struct{})
			for _, profile := range registration.Profiles {
				registered[profile.ID] = struct{}{}
			}
			for profileID := range declared {
				if _, ok := registered[profileID]; !ok {
					t.Errorf("%s classifier profile %q is not registered", registration.Type, profileID)
				}
			}
			for profileID := range registered {
				if _, ok := declared[profileID]; !ok {
					t.Errorf("%s registry profile %q is not classifier-declared", registration.Type, profileID)
				}
			}
		}
	}
}

func TestProductionDestinationConnectorsExposeNoFailureInjectionHooks(t *testing.T) {
	root := filepath.Join(deliveryAuditRoot(t), "connectors", "destinations")
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch declaration := node.(type) {
			case *ast.TypeSpec:
				if declaration.Name.IsExported() && (strings.Contains(declaration.Name.Name, "Hook") || strings.Contains(declaration.Name.Name, "Failpoint")) {
					t.Errorf("exported failure-injection type %s in %s", declaration.Name.Name, path)
				}
			case *ast.FuncDecl:
				if declaration.Name.IsExported() && (strings.Contains(declaration.Name.Name, "Hook") || strings.Contains(declaration.Name.Name, "Failpoint")) {
					t.Errorf("exported failure-injection function %s in %s", declaration.Name.Name, path)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProductionSourcesContainOnlyCurrentTask15Contracts(t *testing.T) {
	root := deliveryAuditRoot(t)
	forbidden := []string{
		"Mark" + "DDL" + "Applied",
		"ManagedTransactionDelivery" + "Coordinator",
		"ManagedSourceFeedback" + "Coordinator",
		"Supports" + "DDL",
		"Support" + "Deprecated",
		"Declared",
		"CheckExternalOverride" + "Allowed",
		"RecordTask" + "Receipt",
		"type Store = connector." + "CheckpointStore",
		"ErrNotFound = connector." + "ErrCheckpointNotFound",
		"func (c *Coordinator) " + "Deliver(",
	}
	var audited int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "site") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".go" && extension != ".proto" {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		audited++
		for _, token := range forbidden {
			if strings.Contains(string(contents), token) {
				t.Errorf("removed compatibility token %q remains in %s", token, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if audited < 100 {
		t.Fatalf("audited only %d production Go/protobuf files", audited)
	}
}
