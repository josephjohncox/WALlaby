package integrationharness

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

type minioRecordingExecutor struct {
	calls    [][]string
	nodes    string
	failVerb string
}

func (e *minioRecordingExecutor) output(_ context.Context, args ...string) ([]byte, error) {
	e.calls = append(e.calls, append([]string(nil), args...))
	if len(args) > 0 && args[0] == e.failVerb {
		return nil, errors.New("injected fixture command failure")
	}
	if len(args) > 0 && args[0] == "ps" {
		return []byte(e.nodes), nil
	}
	return nil, nil
}

func (e *minioRecordingExecutor) input(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	if _, err := io.Copy(io.Discard, input); err != nil {
		return nil, err
	}
	return e.output(ctx, args...)
}

func TestMinioFixtureBuildAndNativeImportAreClusterScoped(t *testing.T) {
	t.Parallel()
	executor := &minioRecordingExecutor{nodes: "owned-control-plane\nowned-worker\n"}
	if err := buildAndLoadMinio(context.Background(), executor, t.TempDir(), "owned", "go1.27.4", "arm64"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(executor.calls[0][:5], []string{"build", "--build-arg", "GO_VERSION=1.27.4", "--tag", minioFixtureImage}) {
		t.Fatalf("fixture build inputs changed: %v", executor.calls[0])
	}
	var archive string
	imports := 0
	for _, args := range executor.calls {
		if args[0] == "image" {
			if args[2] != "--platform=linux/arm64" {
				t.Fatalf("export includes foreign architectures: %v", args)
			}
			archive = args[4]
		}
		if args[0] != "exec" {
			continue
		}
		if args[1] != "-i" || !strings.HasPrefix(args[2], "owned-") {
			t.Fatalf("fixture touched unrelated node or did not stream input: %v", args)
		}
		if args[3] == "ctr" {
			imports++
			if !strings.Contains(strings.Join(args, " "), "--platform=linux/arm64 --digests") {
				t.Fatalf("import can require missing foreign image blobs: %v", args)
			}
		}
	}
	if imports != 2 {
		t.Fatalf("imports=%d, want every selected node", imports)
	}
	if _, err := os.Stat(archive); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary exported image retained: %v", err)
	}
}

func TestMinioFixtureRejectsUnrelatedNodesAndUnsupportedPlatforms(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ cluster, nodes, architecture string }{
		{"owned", "unrelated-control-plane", "arm64"},
		{"owned", "", "arm64"},
		{"owned", "owned-control-plane", "386"},
		{"bad/cluster", "", "amd64"},
	} {
		executor := &minioRecordingExecutor{nodes: test.nodes}
		if err := buildAndLoadMinio(context.Background(), executor, t.TempDir(), test.cluster, "go1.27.4", test.architecture); err == nil {
			t.Fatalf("unsafe fixture configuration accepted: %+v", test)
		}
		for _, args := range executor.calls {
			if args[0] == "cp" || args[0] == "exec" {
				t.Fatalf("invalid ownership reached a node: %v", args)
			}
		}
	}
}

func TestMinioFixtureBuildFailureIsNotAnUpstreamImageFallback(t *testing.T) {
	t.Parallel()
	executor := &minioRecordingExecutor{failVerb: "build"}
	if err := buildAndLoadMinio(context.Background(), executor, t.TempDir(), "owned", "go1.27.4", "amd64"); err == nil {
		t.Fatal("failed source build was ignored")
	}
	if len(executor.calls) != 1 {
		t.Fatalf("build failure attempted another image path: %v", executor.calls)
	}
}
