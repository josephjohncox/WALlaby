package integrationharness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const minioFixtureImage = "wallaby/minio-fixture:07c3a429bfed"

type fixtureExecutor interface {
	output(context.Context, ...string) ([]byte, error)
	input(context.Context, io.Reader, ...string) ([]byte, error)
}

type fixtureDockerExecutor struct{ dockerCommandExecutor }

func (fixtureDockerExecutor) input(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	// #nosec G204 -- Fixed Docker executable, no shell; private fixture callers construct selected-node arguments.
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = input
	return cmd.CombinedOutput()
}

func (h *integrationHarness) prepareMinioFixture() error {
	if !h.config.kindEnabled {
		return errors.New("managed minio fixture requires kind; supply WALLABY_TEST_S3_ENDPOINT for an external service")
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("locate minio fixture build context")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return buildAndLoadMinio(ctx, fixtureDockerExecutor{}, root, h.config.kindCluster, runtime.Version(), runtime.GOARCH)
}

// buildAndLoadMinio builds only the checksum-pinned public source fixture and
// loads the host architecture into this kind cluster. No registry fallback,
// operator image override, credential or cached upstream binary is used.
func buildAndLoadMinio(ctx context.Context, executor fixtureExecutor, root, cluster, goVersion, architecture string) error {
	if cluster == "" || strings.ContainsAny(cluster, " /\\\t\r\n") {
		return errors.New("invalid owned kind cluster name")
	}
	if architecture != "amd64" && architecture != "arm64" {
		return fmt.Errorf("minio fixture does not support architecture %q", architecture)
	}
	if !strings.HasPrefix(goVersion, "go1.") || strings.ContainsAny(goVersion, " /\\\t\r\n") {
		return errors.New("minio fixture requires a release Go toolchain")
	}
	platform := "linux/" + architecture
	buildContext := filepath.Join(root, "tests", "integrationharness", "fixtures", "minio")
	if output, err := executor.output(ctx, "build", "--build-arg", "GO_VERSION="+strings.TrimPrefix(goVersion, "go"), "--tag", minioFixtureImage, buildContext); err != nil {
		return fmt.Errorf("build public minio source fixture: %w: %s", err, output)
	}
	nodesOutput, err := executor.output(ctx, "ps", "--filter", "label=io.x-k8s.kind.cluster="+cluster, "--format", "{{.Names}}")
	if err != nil {
		return fmt.Errorf("list selected kind nodes: %w: %s", err, nodesOutput)
	}
	nodes := strings.Fields(string(nodesOutput))
	if len(nodes) == 0 {
		return errors.New("selected kind cluster has no Docker nodes")
	}
	for _, node := range nodes {
		if !strings.HasPrefix(node, cluster+"-") || strings.ContainsAny(node, "/\\") {
			return errors.New("kind node identity is outside the selected cluster")
		}
	}
	archive, err := os.CreateTemp("", "wallaby-minio-fixture-*.tar")
	if err != nil {
		return err
	}
	archivePath := archive.Name()
	defer func() { _ = os.Remove(archivePath) }()
	if err := archive.Close(); err != nil {
		return err
	}
	if output, err := executor.output(ctx, "image", "save", "--platform="+platform, "--output", archivePath, minioFixtureImage); err != nil {
		return fmt.Errorf("export architecture-filtered minio fixture: %w: %s", err, output)
	}
	for _, node := range nodes {
		if err := importMinioFixture(ctx, executor, node, archivePath, platform); err != nil {
			return err
		}
	}
	return nil
}

func importMinioFixture(ctx context.Context, executor fixtureExecutor, node, archivePath, platform string) error {
	owned, err := os.OpenRoot(filepath.Dir(archivePath))
	if err != nil {
		return err
	}
	defer func() { _ = owned.Close() }()
	archive, err := owned.Open(filepath.Base(archivePath))
	if err != nil {
		return err
	}
	defer func() { _ = archive.Close() }()
	output, err := executor.input(ctx, archive, "exec", "-i", node, "ctr", "--namespace=k8s.io", "images", "import", "--platform="+platform, "--digests", "-")
	if err != nil {
		return fmt.Errorf("import native minio fixture into kind: %w: %s", err, output)
	}
	return nil
}
