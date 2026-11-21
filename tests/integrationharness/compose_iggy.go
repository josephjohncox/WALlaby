package integrationharness

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	"github.com/google/uuid"
	"sigs.k8s.io/yaml"
)

var integrationComposeIggy = flag.Bool("it-compose-iggy", false, "provision an isolated native Iggy Compose service with a Go SDK readiness probe")

type composeCommandExecutor interface {
	output(context.Context, ...string) ([]byte, error)
}

type dockerCommandExecutor struct{}

func (dockerCommandExecutor) output(ctx context.Context, args ...string) ([]byte, error) {
	// #nosec G204 -- Fixed Docker executable without a shell; private callers construct owned-project/fixture arguments.
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

type composeIggySession struct {
	executor composeCommandExecutor
	project  string
	file     string
	dir      string
	original envSnapshot
	evidence composeIggyEvidence
}

type composeIggyEvidence struct {
	Backend   string     `json:"backend"`
	Project   string     `json:"project"`
	Image     string     `json:"image"`
	ImageID   string     `json:"image_id,omitempty"`
	Address   string     `json:"address"`
	Container string     `json:"container_id,omitempty"`
	StartedAt time.Time  `json:"started_at"`
	ReadyAt   time.Time  `json:"ready_at"`
	StoppedAt *time.Time `json:"stopped_at,omitempty"`
}

func materializeIggyFixture(data []byte, port int) ([]byte, string, string, string, error) {
	var config map[string]any
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, "", "", "", err
	}
	services, ok := config["services"].(map[string]any)
	if !ok {
		return nil, "", "", "", errors.New("iggy fixture has no services")
	}
	iggy, ok := services["iggy"].(map[string]any)
	if !ok {
		return nil, "", "", "", errors.New("iggy fixture has no native broker")
	}
	environment, ok := iggy["environment"].(map[string]any)
	if !ok {
		return nil, "", "", "", errors.New("iggy fixture has no explicit development identity")
	}
	image, _ := iggy["image"].(string)
	username, _ := environment["IGGY_ROOT_USERNAME"].(string)
	password, _ := environment["IGGY_ROOT_PASSWORD"].(string)
	if image == "" || username == "" || password == "" || port <= 0 || port > 65535 {
		return nil, "", "", "", errors.New("invalid Iggy fixture image, identity or port")
	}
	// The broker advertises its listening port in metadata. Change BOTH ports,
	// not just the host mapping, so SDK reconnect/routing stays on this service.
	environment["IGGY_TCP_ADDRESS"] = fmt.Sprintf("0.0.0.0:%d", port)
	environment["IGGY_NODE_ADVERTISED_ADDRESS"] = "127.0.0.1"
	iggy["ports"] = []any{fmt.Sprintf("127.0.0.1:%d:%d", port, port)}
	// Readiness is checked by the actual Go SDK below, not by a CLI with a
	// hardcoded default port or by an open TCP socket alone.
	iggy["healthcheck"] = map[string]any{"disable": true}
	result, err := json.MarshalIndent(config, "", "  ")
	return result, image, username, password, err
}

func startComposeIggy(ctx context.Context) (*composeIggySession, error) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return nil, errors.New("locate Iggy fixture source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	data, err := os.ReadFile(filepath.Join(root, "examples", "iggy", "docker-compose.yml"))
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return nil, err
	}
	config, image, username, password, err := materializeIggyFixture(data, port)
	if err != nil {
		return nil, err
	}
	project := "wallaby-live-iggy-" + uuid.NewString()[:12]
	dir := filepath.Join(root, ".cache", "live-evidence", project)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	session := &composeIggySession{executor: dockerCommandExecutor{}, project: project, dir: dir,
		file: filepath.Join(dir, "compose.json"), original: captureEnv("WALLABY_TEST_IGGY_ADDRESS", "WALLABY_TEST_IGGY_USERNAME", "WALLABY_TEST_IGGY_PASSWORD", "WALLABY_TEST_IGGY_TLS"),
		evidence: composeIggyEvidence{Backend: "compose", Project: project, Image: image, Address: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), StartedAt: time.Now().UTC()},
	}
	owned, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = owned.Close() }()
	if err := owned.WriteFile("compose.json", config, 0o600); err != nil {
		return nil, err
	}
	if _, err := session.command(ctx, "up", "--detach"); err != nil {
		_ = session.close()
		return nil, fmt.Errorf("start isolated Iggy fixture: %w", err)
	}
	probe := func(ctx context.Context) error {
		cli, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(session.evidence.Address), tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(username, password))))
		if err != nil {
			return err
		}
		defer func() { _ = cli.Close() }()
		return cli.Connect(ctx)
	}
	if err := waitForNativeIggy(ctx, probe); err != nil {
		logs, _ := session.command(context.WithoutCancel(ctx), "logs", "--tail", "100")
		fmt.Fprintf(os.Stderr, "Iggy readiness diagnostics for owned project %s:\n%s\n", project, logs)
		_ = session.close()
		return nil, err
	}
	session.evidence.ReadyAt = time.Now().UTC()
	container, err := session.command(ctx, "ps", "--quiet", "iggy")
	if err != nil || strings.TrimSpace(string(container)) == "" {
		_ = session.close()
		return nil, fmt.Errorf("record owned Iggy container identity: %w", errors.Join(err, errors.New("missing service container identity")))
	}
	session.evidence.Container = strings.TrimSpace(string(container))
	imageID, err := session.executor.output(ctx, "inspect", "--format", "{{.Image}}", session.evidence.Container)
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(imageID)), "sha256:") {
		_ = session.close()
		return nil, fmt.Errorf("record owned Iggy image identity: %w", errors.Join(err, errors.New("missing content-addressed image identity")))
	}
	session.evidence.ImageID = strings.TrimSpace(string(imageID))
	for key, value := range map[string]string{"WALLABY_TEST_IGGY_ADDRESS": session.evidence.Address, "WALLABY_TEST_IGGY_USERNAME": username, "WALLABY_TEST_IGGY_PASSWORD": password, "WALLABY_TEST_IGGY_TLS": "false"} {
		if err := os.Setenv(key, value); err != nil {
			_ = session.close()
			return nil, err
		}
	}
	if err := session.writeEvidence(); err != nil {
		_ = session.close()
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "native Iggy ready: project=%s address=%s image=%s evidence=%s\n", project, session.evidence.Address, image, dir)
	return session, nil
}

func waitForNativeIggy(ctx context.Context, probe func(context.Context) error) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		last = probe(attempt)
		cancel()
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("native Iggy readiness did not succeed: %w", errors.Join(ctx.Err(), last))
		case <-ticker.C:
		}
	}
}

func (s *composeIggySession) command(ctx context.Context, args ...string) ([]byte, error) {
	command := append([]string{"compose", "--project-name", s.project, "--file", s.file}, args...)
	output, err := s.executor.output(ctx, command...)
	if err != nil {
		return output, fmt.Errorf("docker compose %s for owned project %s: %w\n%s", args[0], s.project, err, output)
	}
	return output, nil
}

func (s *composeIggySession) writeEvidence() error {
	data, err := json.MarshalIndent(s.evidence, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dir, "service.json"), append(data, '\n'), 0o600)
}

func (s *composeIggySession) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.command(ctx, "down", "--remove-orphans")
	if err == nil {
		stopped := time.Now().UTC()
		s.evidence.StoppedAt = &stopped
	}
	s.original.restore()
	return errors.Join(err, s.writeEvidence())
}
