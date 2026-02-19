package integrationharness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/yaml"
)

var (
	integrationKind        = flag.Bool("it-kind", false, "start a local kind cluster for Kubernetes tests")
	integrationKindCluster = flag.String(
		"it-k8s-kind-cluster",
		defaultKindCluster,
		"kind cluster name",
	)
	integrationKindNodeImage = flag.String(
		"it-k8s-kind-node-image",
		defaultKindNodeImage,
		"kind node image",
	)
	integrationKeep                        = flag.Bool("it-keep", false, "keep test infrastructure running after tests")
	integrationExpectedHarnessParticipants = flag.Int(
		"it-expected-harness-participants",
		defaultExpectedHarnessParticipants,
		"number of test packages expected to run under one harness state",
	)
)

const (
	defaultKindCluster                 = "wallaby-test"
	defaultKindNodeImage               = "kindest/node:v1.35.0"
	defaultKindNamespace               = "default"
	defaultServiceReadyTimeout         = 240 * time.Second
	defaultIntegrationLockTimeout      = 5 * time.Minute
	defaultIntegrationHarnessState     = "wallaby-it-integration-harness.state"
	defaultExpectedHarnessParticipants = 1
	defaultIntegrationHarnessStateTTL  = 10 * time.Minute

	defaultPostgresName          = "wallaby-it-postgres"
	defaultPostgresImage         = "postgres:16.11"
	defaultPostgresUser          = "user"
	defaultPostgresPassword      = "pass"
	defaultPostgresDatabase      = "app"
	defaultPostgresPort          = "5432"
	defaultPostgresServicePort   = "5432"
	defaultPostgresLocalBindHost = "127.0.0.1"

	defaultClickHouseName        = "wallaby-it-clickhouse"
	defaultClickHouseImage       = "clickhouse/clickhouse-server:25.12.1.649"
	defaultClickHouseUser        = "wallaby"
	defaultClickHousePassword    = "wallaby"
	defaultClickHouseDatabase    = "default"
	defaultClickHouseServicePort = "9000"
	defaultClickHouseHTTPPort    = "8123"
	defaultClickHouseLocalPort   = "9000"

	defaultMinioName        = "wallaby-it-minio"
	defaultMinioImage       = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
	defaultMinioUser        = "wallaby"
	defaultMinioSecret      = "wallabysecret"
	defaultMinioLocalPort   = "9002"
	defaultMinioServicePort = "9000"

	defaultKafkaName        = "wallaby-it-redpanda"
	defaultKafkaImage       = "docker.redpanda.com/redpandadata/redpanda:v25.3.4"
	defaultKafkaLocalPort   = "9094"
	defaultKafkaServicePort = "9092"

	defaultLocalStackName      = "wallaby-it-localstack"
	defaultLocalStackImage     = "localstack/localstack:3.7.2"
	defaultLocalStackLocalPort = "4566"
	defaultLocalStackRegion    = "us-east-1"

	defaultHTTPTestName        = "wallaby-it-http-test"
	defaultHTTPTestImage       = "python:3.12-slim"
	defaultHTTPTestLocalPort   = "8081"
	defaultHTTPTestServicePort = "8080"

	defaultFakesnowName        = "wallaby-it-fakesnow"
	defaultFakesnowImage       = "python:3.12-slim"
	defaultFakesnowVersion     = "0.11.0"
	defaultFakesnowLocalPort   = "8000"
	defaultFakesnowServicePort = "8000"

	defaultIntegrationHarnessLock = "wallaby-it-integration-harness.lock"
)

var (
	activePostgresLocalPort   = defaultPostgresPort
	activeClickHouseLocalPort = defaultClickHouseLocalPort
	activeMinioLocalPort      = defaultMinioLocalPort
	activeKafkaLocalPort      = defaultKafkaLocalPort
	activeLocalStackLocalPort = defaultLocalStackLocalPort
	activeHTTPTestLocalPort   = defaultHTTPTestLocalPort
	activeFakesnowLocalPort   = defaultFakesnowLocalPort
)

type integrationHarnessConfig struct {
	kindEnabled   bool
	kindKeep      bool
	kindCluster   string
	kindNodeImage string
	expectedPeers int
}

type managedService struct {
	created         bool
	portForwardStop context.CancelFunc
	localPort       string
}

type integrationHarnessSharedState struct {
	Version              int                `json:"version"`
	Active               bool               `json:"active"`
	KindCluster          string             `json:"kindCluster"`
	KindNodeImage        string             `json:"kindNodeImage"`
	KindCreated          bool               `json:"kindCreated"`
	KindKubePath         string             `json:"kindKubePath"`
	KindKeep             bool               `json:"kindKeep"`
	Participants         int                `json:"participants"`
	ExpectedParticipants int                `json:"expectedParticipants"`
	UpdatedAt            time.Time          `json:"updatedAt"`
	Env                  map[string]*string `json:"env"`
}

type integrationHarness struct {
	config       integrationHarnessConfig
	kindCreated  bool
	kindKubePath string
	lockFile     *os.File
	k8sClient    *kubernetes.Clientset
	k8sConfig    *rest.Config
	originalEnv  envSnapshot
	services     map[string]*managedService

	pgCreated         bool
	pgPortForwardStop context.CancelFunc
	pgLocalPort       string
	ownsInfra         bool
	statePath         string
	dumpPodLogsOnFail bool
}

type envSnapshot map[string]*string

func RunIntegrationHarness(m *testing.M) int {
	flag.Parse()

	config := loadIntegrationHarnessConfig()

	h := integrationHarness{config: config}
	h.logf("integration harness config: kind=%v keep=%v cluster=%s nodeImage=%s", h.config.kindEnabled, h.config.kindKeep, h.config.kindCluster, h.config.kindNodeImage)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			if err := h.stop(); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		})
	}
	defer func() {
		signal.Stop(sigCh)
		cleanup()
	}()
	go func() {
		<-sigCh
		cleanup()
		os.Exit(1)
	}()

	if err := h.start(); err != nil {
		h.dumpPodLogsOnFail = true
		fmt.Fprintln(os.Stderr, err)
		cleanup()
		return 1
	}

	result := m.Run()
	if result != 0 {
		h.dumpPodLogsOnFail = true
	}
	cleanup()
	return result
}

func loadIntegrationHarnessConfig() integrationHarnessConfig {
	peers := *integrationExpectedHarnessParticipants
	if peers <= 0 {
		peers = defaultExpectedHarnessParticipants
	}
	return integrationHarnessConfig{
		kindEnabled:   *integrationKind,
		kindKeep:      *integrationKeep,
		kindCluster:   *integrationKindCluster,
		kindNodeImage: *integrationKindNodeImage,
		expectedPeers: peers,
	}
}

func (h *integrationHarness) start() error {
	h.logf("starting integration harness")
	h.originalEnv = captureEnv(integrationManagedEnvKeys()...)
	h.statePath = filepath.Join(os.TempDir(), defaultIntegrationHarnessState)

	if err := h.acquireGlobalLock(); err != nil {
		return err
	}
	state, err := h.readSharedState()
	if err != nil {
		h.releaseGlobalLock()
		return err
	}

	if h.shouldReuseSharedState(state) {
		h.ownsInfra = false
		h.logf("reusing shared harness state for cluster %q", h.config.kindCluster)
		h.restoreManagedEnvFromState(state.Env)
		h.kindKubePath = state.KindKubePath
		h.kindCreated = false
		h.config.kindKeep = h.config.kindKeep || state.KindKeep

		state.Participants++
		state.Active = true
		state.UpdatedAt = time.Now()
		state.ExpectedParticipants = h.config.expectedPeers
		state.KindKeep = h.config.kindKeep
		state.KindCreated = false
		err = h.persistSharedState(state)
		h.releaseGlobalLock()
		if err != nil {
			return err
		}
		return nil
	}

	h.ownsInfra = true
	h.logf("claiming harness ownership for cluster %q", h.config.kindCluster)
	state = &integrationHarnessSharedState{
		Version:              1,
		Active:               true,
		KindCluster:          h.config.kindCluster,
		KindNodeImage:        h.config.kindNodeImage,
		KindCreated:          true,
		KindKubePath:         "",
		KindKeep:             h.config.kindKeep,
		Participants:         1,
		ExpectedParticipants: h.config.expectedPeers,
		UpdatedAt:            time.Now(),
		Env:                  nil,
	}
	if err := h.persistSharedState(state); err != nil {
		h.releaseGlobalLock()
		return err
	}

	if err := h.startKind(); err != nil {
		h.releaseGlobalLock()
		return err
	}
	if err := h.startPostgres(); err != nil {
		h.releaseGlobalLock()
		return err
	}
	if err := h.startManagedDependencies(defaultK8sNamespace()); err != nil {
		h.releaseGlobalLock()
		return err
	}
	setIntegrationDefaults()

	state.KindCreated = h.kindCreated
	state.KindKubePath = h.kindKubePath
	state.Env = currentManagedEnvSnapshot()
	state.UpdatedAt = time.Now()
	state.ExpectedParticipants = h.config.expectedPeers
	state.KindKeep = h.config.kindKeep
	if err := h.persistSharedState(state); err != nil {
		h.releaseGlobalLock()
		return err
	}
	h.releaseGlobalLock()
	return nil
}

func (h *integrationHarness) stop() error {
	if err := h.acquireGlobalLock(); err != nil {
		return err
	}
	defer h.releaseGlobalLock()

	state, err := h.readSharedState()
	if err != nil {
		h.restoreCapturedEnv()
		return err
	}
	if state == nil {
		h.restoreCapturedEnv()
		return nil
	}
	if !state.Active {
		h.restoreCapturedEnv()
		return nil
	}
	if state.KindCluster != h.config.kindCluster {
		h.restoreCapturedEnv()
		return nil
	}

	state.Participants--
	if state.Participants < 0 {
		state.Participants = 0
	}
	state.Active = state.Participants > 0
	state.UpdatedAt = time.Now()
	state.ExpectedParticipants = h.config.expectedPeers
	state.KindKeep = h.config.kindKeep

	shouldDeleteInfrastructure := state.Participants == 0
	if h.dumpPodLogsOnFail {
		h.dumpManagedServiceLogs()
	}
	if shouldDeleteInfrastructure {
		h.stopOwnPostgresAndServices()
		h.deleteManagedInfrastructure()
		state.Active = false
	}

	if shouldDeleteInfrastructure && h.shouldCleanup(state) {
		state.Active = false
		h.cleanupSharedInfrastructure(state)
		_ = os.Remove(h.statePath)
	}

	if err := h.persistSharedState(state); err != nil {
		h.restoreCapturedEnv()
		return err
	}
	h.restoreCapturedEnv()
	return nil
}

func (h *integrationHarness) kubernetesClient() (*kubernetes.Clientset, *rest.Config, error) {
	if h.k8sClient != nil && h.k8sConfig != nil {
		return h.k8sClient, h.k8sConfig, nil
	}

	kubeconfigPath := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	if kubeconfigPath == "" {
		kubeconfigPath = strings.TrimSpace(os.Getenv("KUBECONFIG"))
	}
	if kubeconfigPath == "" || kubeconfigPath == "/dev/null" {
		return nil, nil, fmt.Errorf("no kubeconfig available for kubernetes client")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	h.k8sClient = client
	h.k8sConfig = cfg
	return client, cfg, nil
}

func (h *integrationHarness) startKind() error {
	h.logf("initializing kubernetes endpoint")
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	if kubeconfig != "" {
		h.logf("using existing kubeconfig from WALLABY_TEST_K8S_KUBECONFIG")
		_ = os.Setenv("KUBECONFIG", kubeconfig)
		_ = os.Setenv("WALLABY_TEST_K8S_NAMESPACE", defaultK8sNamespace())
		return nil
	}
	if !h.config.kindEnabled {
		h.logf("skipping kind bootstrap")
		return nil
	}

	ctx := context.Background()
	clusters, err := commandOutput(ctx, "", "kind", "get", "clusters")
	if err != nil {
		return fmt.Errorf("kind unavailable or error reading clusters: %w", err)
	}

	if kindHasCluster(string(clusters), h.config.kindCluster) {
		h.logf("reusing existing kind cluster %q", h.config.kindCluster)
		kubeconfigPath, err := h.getExistingKindKubeconfig(ctx, h.config.kindCluster)
		if err != nil {
			return fmt.Errorf("resolve existing kind cluster %q kubeconfig: %w", h.config.kindCluster, err)
		}
		if err := h.validateKindCluster(kubeconfigPath); err == nil {
			h.kindKubePath = kubeconfigPath
			_ = os.Setenv("WALLABY_TEST_K8S_KUBECONFIG", h.kindKubePath)
			_ = os.Setenv("KUBECONFIG", h.kindKubePath)
			_ = os.Setenv("WALLABY_TEST_K8S_NAMESPACE", defaultK8sNamespace())
			return nil
		}
		h.logf("existing kind cluster %q appears unhealthy: %v", h.config.kindCluster, err)
		if destroyErr := h.deleteKindCluster(ctx, h.config.kindCluster); destroyErr != nil && harnessVerbose() {
			fmt.Fprintf(os.Stderr, "failed to cleanup stale kind cluster %q: %v\n", h.config.kindCluster, destroyErr)
		}
	}

	h.kindCreated = true
	kubeconfigPath, err := h.createKindCluster(ctx)
	if err != nil {
		return fmt.Errorf("create kind cluster %q: %w", h.config.kindCluster, err)
	}
	h.kindKubePath = kubeconfigPath

	if h.kindKubePath == "" {
		return fmt.Errorf("kind cluster %q returned empty kubeconfig path", h.config.kindCluster)
	}
	_ = os.Setenv("WALLABY_TEST_K8S_KUBECONFIG", h.kindKubePath)
	_ = os.Setenv("KUBECONFIG", h.kindKubePath)
	_ = os.Setenv("WALLABY_TEST_K8S_NAMESPACE", defaultK8sNamespace())

	return nil
}

func (h *integrationHarness) validateKindCluster(kubeconfigPath string) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{Limit: 1})
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}

func (h *integrationHarness) createKindCluster(ctx context.Context) (string, error) {
	h.logf("creating kind cluster %q (image=%q)", h.config.kindCluster, h.config.kindNodeImage)
	args := []string{"create", "cluster", "--name", h.config.kindCluster}
	if strings.TrimSpace(h.config.kindNodeImage) != "" {
		args = append(args, "--image", h.config.kindNodeImage)
	}

	_, createErr := commandOutput(ctx, "", "kind", args...)
	if createErr != nil {
		return "", createErr
	}

	kubeconfigPath, kubeErr := h.getExistingKindKubeconfig(ctx, h.config.kindCluster)
	if kubeErr != nil {
		return "", kubeErr
	}
	if vErr := h.validateKindCluster(kubeconfigPath); vErr != nil {
		return "", vErr
	}

	return kubeconfigPath, nil
}

func (h *integrationHarness) deleteKindCluster(ctx context.Context, cluster string) error {
	_, err := commandOutput(ctx, "", "kind", "delete", "cluster", "--name", cluster)
	return err
}

func (h *integrationHarness) getExistingKindKubeconfig(ctx context.Context, clusterName string) (string, error) {
	kubeconfigPath := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	if kubeconfigPath != "" {
		_, err := os.Stat(kubeconfigPath)
		if err == nil {
			return kubeconfigPath, nil
		}
	}

	raw, err := commandOutput(ctx, "", "kind", "get", "kubeconfig", "--name", clusterName)
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "", fmt.Errorf("empty kubeconfig for existing kind cluster %q", clusterName)
	}
	temp, err := os.CreateTemp("", fmt.Sprintf("wallaby-%s-kubeconfig-*", clusterName))
	if err != nil {
		return "", fmt.Errorf("create temp kubeconfig: %w", err)
	}
	if _, err := temp.WriteString(trimmed); err != nil {
		_ = os.Remove(temp.Name())
		if closeErr := temp.Close(); closeErr != nil {
			return "", fmt.Errorf("write temp kubeconfig: %w", errors.Join(err, closeErr))
		}
		return "", fmt.Errorf("write temp kubeconfig: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		return "", fmt.Errorf("flush temp kubeconfig: %w", err)
	}

	return temp.Name(), nil
}

func (h *integrationHarness) startPostgres() error {
	h.logf("ensuring postgres dependency")
	if strings.TrimSpace(os.Getenv("TEST_PG_DSN")) != "" {
		h.logf("TEST_PG_DSN already set; skipping postgres bootstrap")
		h.pgLocalPort = ""
		return nil
	}
	if !h.hasK8sEndpoint() {
		h.logf("no kubernetes endpoint; skipping postgres bootstrap")
		h.pgLocalPort = ""
		return nil
	}

	activePostgresLocalPort = defaultPostgresPort
	if !isPortAvailable(defaultPostgresLocalBindHost, defaultPostgresPort) {
		return fmt.Errorf("local postgres port %s:%s is already in use", defaultPostgresLocalBindHost, defaultPostgresPort)
	}

	namespace := defaultK8sNamespace()
	if err := h.deployPostgres(namespace); err != nil {
		return fmt.Errorf("start postgres in kind: %w", err)
	}
	if err := h.waitForServiceReady(namespace, defaultPostgresName); err != nil {
		return err
	}
	var err error
	activePostgresLocalPort, err = h.startPostgresPortForward(namespace, activePostgresLocalPort)
	if err != nil {
		return err
	}
	h.pgLocalPort = activePostgresLocalPort
	_ = os.Setenv("TEST_PG_DSN", defaultTestPostgresDSN())
	return nil
}

func (h *integrationHarness) startManagedDependencies(namespace string) error {
	h.logf("ensuring managed services in namespace %s", namespace)
	if !h.hasK8sEndpoint() {
		h.logf("no kubernetes endpoint; skipping managed services")
		return nil
	}

	if err := h.startClickHouse(namespace); err != nil {
		return err
	}
	if err := h.startS3(namespace); err != nil {
		return err
	}
	if err := h.startKafka(namespace); err != nil {
		return err
	}
	if err := h.startLocalStack(namespace); err != nil {
		return err
	}
	if err := h.startHTTPTestService(namespace); err != nil {
		return err
	}
	if err := h.startFakesnow(namespace); err != nil {
		return err
	}
	return nil
}

func (h *integrationHarness) startClickHouse(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")) != "" {
		return nil
	}
	manifest, err := clickhouseKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultClickHouseName,
		defaultClickHouseLocalPort,
		defaultClickHouseServicePort,
		manifest,
		"clickhouse",
	)
	if err != nil {
		return err
	}
	activeClickHouseLocalPort = localPort
	setenv("WALLABY_TEST_CLICKHOUSE_DSN", defaultClickHouseDSN())
	setenv("WALLABY_TEST_CLICKHOUSE_DB", getenvString("WALLABY_TEST_CLICKHOUSE_DB", defaultClickHouseDatabase))
	setenv("TEST_CLICKHOUSE_HTTP_PORT", getenvString("TEST_CLICKHOUSE_HTTP_PORT", defaultClickHouseHTTPPort))
	return nil
}

func (h *integrationHarness) startS3(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_S3_ENDPOINT")) != "" {
		return nil
	}
	manifest, err := minioKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultMinioName,
		defaultMinioLocalPort,
		defaultMinioServicePort,
		manifest,
		"minio",
	)
	if err != nil {
		return err
	}
	activeMinioLocalPort = localPort
	setenv("WALLABY_TEST_S3_ENDPOINT", localURL(defaultPostgresLocalBindHost, activeMinioLocalPort))
	setenv("WALLABY_TEST_S3_BUCKET", getenvString("WALLABY_TEST_S3_BUCKET", "wallaby-test"))
	setenv("WALLABY_TEST_S3_ACCESS_KEY", getenvString("WALLABY_TEST_S3_ACCESS_KEY", "wallaby"))
	setenv("WALLABY_TEST_S3_SECRET_KEY", getenvString("WALLABY_TEST_S3_SECRET_KEY", "wallabysecret"))
	setenv("WALLABY_TEST_S3_REGION", getenvString("WALLABY_TEST_S3_REGION", "us-east-1"))
	return nil
}

func (h *integrationHarness) startKafka(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_KAFKA_BROKERS")) != "" {
		return nil
	}
	manifest, err := kafkaKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultKafkaName,
		defaultKafkaLocalPort,
		defaultKafkaServicePort,
		manifest,
		"kafka",
	)
	if err != nil {
		return err
	}
	activeKafkaLocalPort = localPort
	setenv("WALLABY_TEST_KAFKA_BROKERS", localBrokers())
	return nil
}

func (h *integrationHarness) startLocalStack(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_GLUE_ENDPOINT")) != "" {
		return nil
	}
	manifest, err := localStackKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultLocalStackName,
		defaultLocalStackLocalPort,
		defaultLocalStackLocalPort,
		manifest,
		"localstack",
	)
	if err != nil {
		return err
	}
	activeLocalStackLocalPort = localPort
	setenv("WALLABY_TEST_GLUE_ENDPOINT", localURL(defaultPostgresLocalBindHost, activeLocalStackLocalPort))
	setenv("WALLABY_TEST_GLUE_REGION", getenvString("WALLABY_TEST_GLUE_REGION", defaultLocalStackRegion))
	return nil
}

func (h *integrationHarness) startHTTPTestService(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_HTTP_URL")) != "" {
		return nil
	}
	manifest, err := httpTestKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultHTTPTestName,
		defaultHTTPTestLocalPort,
		defaultHTTPTestServicePort,
		manifest,
		"http-test",
	)
	if err != nil {
		return err
	}
	activeHTTPTestLocalPort = localPort
	setenv("WALLABY_TEST_HTTP_URL", localURL(defaultPostgresLocalBindHost, activeHTTPTestLocalPort))
	return nil
}

func (h *integrationHarness) startFakesnow(namespace string) error {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_HOST")) != "" || strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PORT")) != "" {
		return nil
	}
	if !shouldStartFakesnow() {
		return nil
	}
	manifest, err := fakesnowKindManifest(namespace)
	if err != nil {
		return err
	}
	localPort, err := h.startManagedService(
		namespace,
		defaultFakesnowName,
		defaultFakesnowLocalPort,
		defaultFakesnowServicePort,
		manifest,
		"fakesnow",
	)
	if err != nil {
		return err
	}
	setenv("WALLABY_TEST_FAKESNOW_HOST", defaultPostgresLocalBindHost)
	activeFakesnowLocalPort = localPort
	setenv("WALLABY_TEST_FAKESNOW_PORT", activeFakesnowLocalPort)
	return nil
}

func (h *integrationHarness) stopOwnPostgresAndServices() {
	if !h.ownsInfra {
		return
	}

	if h.pgPortForwardStop != nil {
		h.pgPortForwardStop()
		h.pgPortForwardStop = nil
		if h.pgLocalPort != "" {
			waitForPortRelease(defaultPostgresLocalBindHost, h.pgLocalPort, 5*time.Second)
		}
	}
	h.stopManagedServices()
}

func (h *integrationHarness) deleteManagedInfrastructure() {
	namespace := defaultK8sNamespace()
	h.cleanupPostgres()
	_ = h.deleteServiceAndDeployment(namespace, defaultClickHouseName)
	_ = h.deleteServiceAndDeployment(namespace, defaultMinioName)
	_ = h.deleteServiceAndDeployment(namespace, defaultKafkaName)
	_ = h.deleteServiceAndDeployment(namespace, defaultLocalStackName)
	_ = h.deleteServiceAndDeployment(namespace, defaultHTTPTestName)
	_ = h.deleteServiceAndDeployment(namespace, defaultFakesnowName)
}

func (h *integrationHarness) readSharedState() (*integrationHarnessSharedState, error) {
	raw, err := os.ReadFile(h.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &integrationHarnessSharedState{}, nil
		}
		return nil, fmt.Errorf("read harness shared state %s: %w", h.statePath, err)
	}
	if len(raw) == 0 {
		return &integrationHarnessSharedState{}, nil
	}
	var state integrationHarnessSharedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode harness shared state %s: %w", h.statePath, err)
	}
	return &state, nil
}

func (h *integrationHarness) persistSharedState(state *integrationHarnessSharedState) error {
	if state == nil {
		state = &integrationHarnessSharedState{}
	}
	state.UpdatedAt = time.Now()
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal harness shared state: %w", err)
	}
	tmp := h.statePath + ".tmp"
	if err := os.WriteFile(tmp, encoded, 0o600); err != nil {
		return fmt.Errorf("write harness shared state %s: %w", tmp, err)
	}
	return os.Rename(tmp, h.statePath)
}

func (h *integrationHarness) shouldReuseSharedState(state *integrationHarnessSharedState) bool {
	if state == nil {
		return false
	}
	if state.Version <= 0 && state.Active {
		return false
	}
	if !state.Active {
		return false
	}
	if strings.TrimSpace(state.KindCluster) != strings.TrimSpace(h.config.kindCluster) {
		return false
	}
	if state.Participants <= 0 {
		return false
	}
	if !state.UpdatedAt.IsZero() && time.Since(state.UpdatedAt) > defaultIntegrationHarnessStateTTL {
		return false
	}
	return true
}

func (h *integrationHarness) restoreManagedEnvFromState(stateEnv map[string]*string) {
	if stateEnv == nil {
		return
	}
	for _, key := range integrationManagedEnvKeys() {
		v, ok := stateEnv[key]
		if !ok || v == nil {
			_ = os.Unsetenv(key)
			continue
		}
		_ = os.Setenv(key, *v)
	}
}

func currentManagedEnvSnapshot() map[string]*string {
	return captureEnv(integrationManagedEnvKeys()...)
}

func (h *integrationHarness) shouldCleanup(state *integrationHarnessSharedState) bool {
	if h.config.kindKeep || state.KindKeep {
		return false
	}
	if state == nil {
		return false
	}
	return state.Participants == 0 && !state.Active
}

func (h *integrationHarness) cleanupSharedInfrastructure(state *integrationHarnessSharedState) {
	if state == nil {
		return
	}
	if state.KindCreated && !state.KindKeep && !h.config.kindKeep {
		if err := h.deleteKindCluster(context.Background(), h.config.kindCluster); err != nil && harnessVerbose() {
			fmt.Fprintf(os.Stderr, "failed to delete kind cluster %s: %v\n", h.config.kindCluster, err)
		}
	}
	if state.KindKubePath != "" {
		_ = os.Remove(state.KindKubePath)
	}
}

func (h *integrationHarness) restoreCapturedEnv() {
	if h.originalEnv != nil {
		h.originalEnv.restore()
	}
}

func (h *integrationHarness) acquireGlobalLock() error {
	lockPath := filepath.Join(os.TempDir(), defaultIntegrationHarnessLock)
	// #nosec G304 -- lockPath is a fixed lock filename in the temporary directory.
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("acquire integration harness lock: open lock file %s: %w", lockPath, err)
	}
	h.lockFile = lockFile
	deadline := time.Now().Add(defaultIntegrationLockTimeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("acquire integration harness lock: timeout waiting for %s", lockPath)
		}
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (h *integrationHarness) releaseGlobalLock() {
	if h.lockFile == nil {
		return
	}
	_ = syscall.Flock(int(h.lockFile.Fd()), syscall.LOCK_UN)
	_ = h.lockFile.Close()
	h.lockFile = nil
}

func (h *integrationHarness) stopManagedServices() {
	for service := range h.services {
		h.stopManagedService(service)
	}
}

func (h *integrationHarness) stopManagedService(name string) {
	svc, ok := h.services[name]
	if !ok {
		return
	}
	namespace := defaultK8sNamespace()

	if svc.portForwardStop != nil {
		svc.portForwardStop()
		svc.portForwardStop = nil
		if svc.localPort != "" {
			waitForPortRelease(defaultPostgresLocalBindHost, svc.localPort, 5*time.Second)
		}
	}
	if svc.created {
		_ = h.deleteServiceAndDeployment(namespace, name)
		svc.created = false
	}
	delete(h.services, name)
}

func integrationManagedEnvKeys() []string {
	return []string{
		"WALLABY_TEST_K8S_NAMESPACE",
		"WALLABY_TEST_K8S_KUBECONFIG",
		"KUBECONFIG",
		"TEST_PG_DSN",
		"WALLABY_TEST_DBOS_DSN",
		"WALLABY_TEST_CLICKHOUSE_DSN",
		"WALLABY_TEST_CLICKHOUSE_DB",
		"TEST_CLICKHOUSE_HTTP_PORT",
		"WALLABY_TEST_FAKESNOW_HOST",
		"WALLABY_TEST_FAKESNOW_PORT",
		"WALLABY_TEST_FORCE_FAKESNOW",
		"WALLABY_TEST_RUN_FAKESNOW",
		"WALLABY_TEST_CLI_LOG",
		"WALLABY_TEST_S3_ENDPOINT",
		"WALLABY_TEST_S3_BUCKET",
		"WALLABY_TEST_S3_ACCESS_KEY",
		"WALLABY_TEST_S3_SECRET_KEY",
		"WALLABY_TEST_S3_REGION",
		"WALLABY_TEST_DUCKLAKE",
		"WALLABY_TEST_KAFKA_BROKERS",
		"WALLABY_TEST_HTTP_URL",
		"WALLABY_TEST_GLUE_ENDPOINT",
		"WALLABY_TEST_GLUE_REGION",
		"WALLABY_TEST_SNOWFLAKE_DSN",
		"WALLABY_TEST_SNOWFLAKE_SCHEMA",
		"WALLABY_TEST_SNOWPIPE_DSN",
		"WALLABY_TEST_SNOWPIPE_STAGE",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_PROFILE",
		"AWS_DEFAULT_PROFILE",
		"AWS_CONFIG_FILE",
		"AWS_SHARED_CREDENTIALS_FILE",
		"AWS_EC2_METADATA_DISABLED",
		"AWS_SDK_LOAD_CONFIG",
	}
}

func (h *integrationHarness) getService(name string) *managedService {
	if h.services == nil {
		h.services = make(map[string]*managedService)
	}
	if svc, ok := h.services[name]; ok {
		return svc
	}
	svc := &managedService{}
	h.services[name] = svc
	return svc
}

func (h *integrationHarness) startManagedService(namespace, name, localPort, servicePort, manifest, label string) (string, error) {
	h.logf("starting managed service %s", name)
	exists := h.hasExistingManagedService(namespace, name)

	if _, ok := h.services[name]; !ok {
		h.getService(name)
	}
	if err := h.applyManifest(namespace, manifest); err != nil {
		return "", fmt.Errorf("start %s: %w", label, err)
	}
	svc := h.getService(name)
	svc.created = !exists
	svc.localPort = ""

	if err := h.waitForServiceReady(namespace, name); err != nil {
		return "", fmt.Errorf("start %s: %w", label, err)
	}
	resolvedLocalPort, portForwardStop, err := h.startLocalPortForward(namespace, name, localPort, servicePort)
	if err != nil {
		return "", fmt.Errorf("start %s: %w", label, err)
	}
	svc.localPort = resolvedLocalPort
	svc.portForwardStop = portForwardStop
	return resolvedLocalPort, nil
}

func (h *integrationHarness) deleteServiceAndDeployment(namespace, name string) error {
	client, _, err := h.kubernetesClient()
	if err != nil {
		return fmt.Errorf("connect to kubernetes for deleting service/deployment %s: %w", name, err)
	}
	ctx := context.Background()
	deleteOptions := metav1.DeleteOptions{
		PropagationPolicy: func() *metav1.DeletionPropagation {
			policy := metav1.DeletePropagationForeground
			return &policy
		}(),
	}
	if err := client.CoreV1().Services(namespace).Delete(ctx, name, deleteOptions); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete service %s: %w", name, err)
	}
	if err := client.AppsV1().Deployments(namespace).Delete(ctx, name, deleteOptions); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete deployment %s: %w", name, err)
	}
	return nil
}

func (h *integrationHarness) applyManifest(namespace, manifest string) error {
	h.logf("applying manifest for namespace %s", namespace)
	if strings.TrimSpace(manifest) == "" {
		return fmt.Errorf("manifest is empty")
	}

	client, _, err := h.kubernetesClient()
	if err != nil {
		return fmt.Errorf("connect to kubernetes for manifest apply: %w", err)
	}

	parts := strings.Split(manifest, "\n---")
	for _, part := range parts {
		raw := strings.TrimSpace(part)
		if raw == "" {
			continue
		}

		var kindMeta struct {
			Kind       string `yaml:"kind"`
			APIVersion string `yaml:"apiVersion"`
		}
		if err := yaml.Unmarshal([]byte(raw), &kindMeta); err != nil {
			return fmt.Errorf("decode manifest metadata: %w", err)
		}
		if kindMeta.APIVersion == "" {
			return fmt.Errorf("manifest missing apiVersion")
		}

		ctx := context.Background()
		switch kindMeta.Kind {
		case "Service":
			var service corev1.Service
			if err := yaml.Unmarshal([]byte(raw), &service); err != nil {
				return fmt.Errorf("decode service manifest: %w", err)
			}
			existing, err := client.CoreV1().Services(service.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
			if err == nil {
				service.ResourceVersion = existing.ResourceVersion
				if _, err := client.CoreV1().Services(service.Namespace).Update(ctx, &service, metav1.UpdateOptions{}); err != nil {
					return fmt.Errorf("update service %s: %w", service.Name, err)
				}
				continue
			}
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("read service %s: %w", service.Name, err)
			}
			if _, err := client.CoreV1().Services(service.Namespace).Create(ctx, &service, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("create service %s: %w", service.Name, err)
			}
		case "Deployment":
			var deployment appsv1.Deployment
			if err := yaml.Unmarshal([]byte(raw), &deployment); err != nil {
				return fmt.Errorf("decode deployment manifest: %w", err)
			}
			existing, err := client.AppsV1().Deployments(deployment.Namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
			if err == nil {
				deployment.ResourceVersion = existing.ResourceVersion
				if _, err := client.AppsV1().Deployments(deployment.Namespace).Update(ctx, &deployment, metav1.UpdateOptions{}); err != nil {
					return fmt.Errorf("update deployment %s: %w", deployment.Name, err)
				}
				continue
			}
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("read deployment %s: %w", deployment.Name, err)
			}
			if _, err := client.AppsV1().Deployments(deployment.Namespace).Create(ctx, &deployment, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("create deployment %s: %w", deployment.Name, err)
			}
		default:
			return fmt.Errorf("unsupported manifest kind %q", kindMeta.Kind)
		}
	}
	return nil
}

func (h *integrationHarness) waitForServiceReady(namespace, name string) error {
	timeout := serviceReadyTimeout()
	h.logf("waiting for service %s to become ready (timeout=%s)", name, timeout)
	client, _, err := h.kubernetesClient()
	if err != nil {
		return fmt.Errorf("connect to kubernetes for readiness check of %s: %w", name, err)
	}

	deadline := time.Now().Add(timeout)
	label := fmt.Sprintf("app=%s", name)
	var lastWaitOutput []byte
	for {
		deploymentReady := false
		podReadyCount := 0
		podTotal := 0
		var waitErr error

		deployment, depErr := client.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if depErr == nil {
			expected := int32(1)
			if deployment.Spec.Replicas != nil {
				expected = *deployment.Spec.Replicas
			}
			deploymentReady = deployment.Status.ReadyReplicas >= expected && expected > 0
			lastWaitOutput = []byte(fmt.Sprintf("deployment ready_replicas=%d expected=%d", deployment.Status.ReadyReplicas, expected))
		} else {
			lastWaitOutput = []byte(fmt.Sprintf("deployment lookup: %v", depErr))
			waitErr = depErr
		}

		pods, podErr := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: label,
		})
		if podErr == nil {
			podTotal = len(pods.Items)
			for _, pod := range pods.Items {
				if isPodReady(&pod) {
					podReadyCount++
				}
			}
			if podTotal > 0 {
				lastWaitOutput = []byte(fmt.Sprintf("%s; pods ready=%d total=%d", strings.TrimSpace(string(lastWaitOutput)), podReadyCount, podTotal))
			}
		} else {
			lastWaitOutput = []byte(fmt.Sprintf("%s; pods lookup: %v", strings.TrimSpace(string(lastWaitOutput)), podErr))
			if waitErr == nil {
				waitErr = podErr
			}
		}

		if deploymentReady {
			h.printServiceReadyStatus(namespace, name)
			return nil
		}

		// Fallback: tolerate transient missing deployment objects as long as a ready pod
		// exists for the service label selector.
		if podErr == nil && podReadyCount > 0 && depErr != nil {
			h.printServiceReadyStatus(namespace, name)
			return waitErr
		}

		if time.Now().After(deadline) {
			h.printServiceDiagnostics(namespace, name, timeout, lastWaitOutput, waitErr)
			if waitErr != nil {
				return waitErr
			}
			return fmt.Errorf("service %s did not become ready within %s", name, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *integrationHarness) printServiceReadyStatus(namespace, name string) {
	if !harnessVerbose() {
		return
	}
	label := fmt.Sprintf("app=%s", name)
	client, _, err := h.kubernetesClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "service %s ready but kubernetes client unavailable: %v\n", name, err)
		return
	}
	pods, podErr := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: label})
	if podErr != nil || len(pods.Items) == 0 {
		fmt.Fprintf(os.Stderr, "service %s ready but pod list unavailable: %v\n", name, podErr)
		return
	}
	var podNames []string
	for _, pod := range pods.Items {
		podNames = append(podNames, pod.Name)
	}
	fmt.Fprintf(os.Stderr, "service %s ready with pods: %s\n", name, strings.Join(podNames, ", "))
	pod := pods.Items[0]
	var ready, restartTotal int32
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Ready {
			ready++
		}
		restartTotal += containerStatus.RestartCount
	}
	fmt.Fprintf(os.Stderr, "pod status:\n")
	fmt.Fprintf(os.Stderr, "%s\t%d/%d\t%s\t%d\n", pod.Name, ready, len(pod.Status.ContainerStatuses), pod.Status.Phase, restartTotal)
}

func (h *integrationHarness) printServiceDiagnostics(namespace, name string, timeout time.Duration, waitOutput []byte, waitErr error) {
	label := fmt.Sprintf("app=%s", name)
	client, _, err := h.kubernetesClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "service %s failed to become ready within %s: %v\n", name, timeout, waitErr)
		if len(waitOutput) > 0 {
			fmt.Fprintln(os.Stderr, "service readiness output:", strings.TrimSpace(string(waitOutput)))
		}
		fmt.Fprintf(os.Stderr, "unable to initialize kubernetes client: %v\n", err)
		return
	}
	podList, podErr := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: label})

	fmt.Fprintf(os.Stderr, "service %s failed to become ready within %s: %v\n", name, timeout, waitErr)
	if len(waitOutput) > 0 {
		fmt.Fprintln(os.Stderr, "service readiness output:", strings.TrimSpace(string(waitOutput)))
	}
	if podErr == nil && len(podList.Items) > 0 {
		var podNames []string
		for _, pod := range podList.Items {
			podNames = append(podNames, pod.Name)
		}
		fmt.Fprintf(os.Stderr, "service %s pods: %s\n", name, strings.Join(podNames, ", "))
		pretty, prettyErr := json.MarshalIndent(podList.Items[0], "", "  ")
		if prettyErr == nil {
			fmt.Fprintln(os.Stderr, "pod:")
			fmt.Fprintln(os.Stderr, string(pretty))
		} else {
			fmt.Fprintf(os.Stderr, "failed to serialize pod diagnostics for %s: %v\n", podList.Items[0].Name, prettyErr)
		}
	} else {
		if podErr != nil {
			fmt.Fprintf(os.Stderr, "unable to list pods for %s: %v\n", name, podErr)
		} else {
			fmt.Fprintf(os.Stderr, "no pods currently found for label %s\n", label)
		}
	}
}

func serviceReadyTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("WALLABY_IT_SERVICE_READY_TIMEOUT_SECONDS"))
	if raw == "" {
		return defaultServiceReadyTimeout
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		fmt.Fprintf(os.Stderr, "invalid WALLABY_IT_SERVICE_READY_TIMEOUT_SECONDS=%q; using %s\n", raw, defaultServiceReadyTimeout)
		return defaultServiceReadyTimeout
	}
	return time.Duration(seconds) * time.Second
}

func (h *integrationHarness) pickPodForService(namespace, name string) (string, error) {
	client, _, err := h.kubernetesClient()
	if err != nil {
		return "", fmt.Errorf("connect to kubernetes for selecting pod for %s: %w", name, err)
	}
	label := fmt.Sprintf("app=%s", name)
	deadline := time.Now().Add(serviceReadyTimeout())

	for {
		podList, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: label,
		})
		if err == nil {
			for _, pod := range podList.Items {
				if isPodReady(&pod) {
					return pod.Name, nil
				}
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return "", fmt.Errorf("selecting pod for %s: %w", name, err)
			}
			return "", fmt.Errorf("no ready pod found for service %s", name)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (h *integrationHarness) startPodPortForward(namespace, podName, localPort, servicePort string) (context.CancelFunc, error) {
	client, cfg, err := h.kubernetesClient()
	if err != nil {
		return nil, fmt.Errorf("connect to kubernetes for pod %s: %w", podName, err)
	}

	requestURL := client.CoreV1().RESTClient().Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("prepare port-forward transport for pod %s: %w", podName, err)
	}

	dialer := spdy.NewDialer(
		upgrader,
		&http.Client{
			Transport: transport,
		},
		http.MethodPost,
		requestURL,
	)

	// Keep port-forward runtime chatter silent during normal execution.
	// Service logs are only dumped once via dumpPodLogsOnFail in stop().
	out := io.Discard
	errOut := io.Discard

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	readyErr := make(chan error, 1)
	forwarder, err := portforward.NewOnAddresses(
		dialer,
		[]string{defaultPostgresLocalBindHost},
		[]string{fmt.Sprintf("%s:%s", localPort, servicePort)},
		ctx.Done(),
		ready,
		out,
		errOut,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create port-forwarder for pod %s: %w", podName, err)
	}

	go func() {
		readyErr <- forwarder.ForwardPorts()
	}()

	select {
	case <-ready:
		return cancel, nil
	case err := <-readyErr:
		cancel()
		if err != nil {
			return nil, fmt.Errorf("run port-forward for pod %s: %w", podName, err)
		}
		return nil, fmt.Errorf("port-forward for pod %s terminated unexpectedly", podName)
	case <-time.After(30 * time.Second):
		cancel()
		return nil, fmt.Errorf("timed out starting port-forward for pod %s", podName)
	}
}

func (h *integrationHarness) printPodLogs(namespace, pod string) {
	logs, logsErr := h.fetchPodLogs(namespace, pod, false)
	if logsErr == nil && len(logs) > 0 {
		fmt.Fprintf(os.Stderr, "pod logs (%s):\n%s\n", pod, strings.TrimSpace(logs))
	}

	prevLogs, prevErr := h.fetchPodLogs(namespace, pod, true)
	if prevErr == nil && len(prevLogs) > 0 {
		fmt.Fprintf(os.Stderr, "pod logs (%s, previous):\n%s\n", pod, strings.TrimSpace(prevLogs))
	}
}

func (h *integrationHarness) dumpManagedServiceLogs() {
	if !h.dumpPodLogsOnFail {
		return
	}
	if !h.ownsInfra {
		return
	}
	namespace := defaultK8sNamespace()

	serviceNames := h.managedServiceNamesForDump()
	if len(serviceNames) == 0 {
		return
	}

	fmt.Fprintln(os.Stderr, "[it] integration test failure detected; dumping service logs")
	for _, serviceName := range serviceNames {
		h.dumpPodLogsForService(namespace, serviceName)
	}
}

func (h *integrationHarness) managedServiceNamesForDump() []string {
	serviceNameSet := map[string]struct{}{}
	if h.pgCreated || h.pgLocalPort != "" {
		serviceNameSet[defaultPostgresName] = struct{}{}
	}
	for serviceName := range h.services {
		serviceNameSet[serviceName] = struct{}{}
	}

	serviceNames := make([]string, 0, len(serviceNameSet))
	for serviceName := range serviceNameSet {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)
	return serviceNames
}

func (h *integrationHarness) dumpPodLogsForService(namespace, serviceName string) {
	client, _, err := h.kubernetesClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to kubernetes for %s logs: %v\n", serviceName, err)
		return
	}

	podList, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", serviceName),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to list pods for %s logs: %v\n", serviceName, err)
		return
	}

	if len(podList.Items) == 0 {
		fmt.Fprintf(os.Stderr, "service %s had no pods to log\n", serviceName)
		return
	}

	fmt.Fprintf(os.Stderr, "service %s logs:\n", serviceName)
	for _, pod := range podList.Items {
		h.printPodLogs(namespace, pod.Name)
	}
}

func (h *integrationHarness) fetchPodLogs(namespace, pod string, previous bool) (string, error) {
	client, _, err := h.kubernetesClient()
	if err != nil {
		return "", err
	}

	tail := int64(160)
	req := client.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		TailLines: &tail,
		Previous:  previous,
	})
	stream, err := req.Stream(context.Background())
	if err != nil {
		return "", err
	}
	defer func() {
		if err := stream.Close(); err != nil && harnessVerbose() {
			fmt.Fprintf(os.Stderr, "failed to close logs stream for %s: %v\n", pod, err)
		}
	}()

	data, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func captureEnv(keys ...string) envSnapshot {
	result := make(envSnapshot, len(keys))
	for _, key := range keys {
		value, set := os.LookupEnv(key)
		if !set {
			result[key] = nil
			continue
		}
		v := value
		result[key] = &v
	}
	return result
}

func (snapshot envSnapshot) restore() {
	for key, value := range snapshot {
		if value == nil {
			_ = os.Unsetenv(key)
			continue
		}
		_ = os.Setenv(key, *value)
	}
}

func (h *integrationHarness) hasK8sEndpoint() bool {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG")) != "" {
		return true
	}
	if strings.TrimSpace(os.Getenv("KUBECONFIG")) != "" &&
		strings.TrimSpace(os.Getenv("KUBECONFIG")) != "/dev/null" {
		return true
	}
	return strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
}

func shouldStartFakesnow() bool {
	return strings.TrimSpace(os.Getenv("WALLABY_TEST_RUN_FAKESNOW")) == "1" ||
		strings.TrimSpace(os.Getenv("WALLABY_TEST_FORCE_FAKESNOW")) == "1"
}

func (h *integrationHarness) deployPostgres(namespace string) error {
	manifestDoc, err := postgresKindManifest(namespace)
	if err != nil {
		return err
	}
	if err := h.applyManifest(namespace, manifestDoc); err != nil {
		return err
	}
	h.pgCreated = true
	return nil
}

func (h *integrationHarness) cleanupPostgres() {
	if !h.pgCreated {
		return
	}
	namespace := defaultK8sNamespace()
	_ = h.deleteServiceAndDeployment(namespace, defaultPostgresName)
	h.pgCreated = false
}

func setIntegrationDefaults() {
	setenv("WALLABY_TEST_FORCE_FAKESNOW", getenvString("WALLABY_TEST_FORCE_FAKESNOW", "0"))
	setenv("WALLABY_TEST_RUN_FAKESNOW", getenvString("WALLABY_TEST_RUN_FAKESNOW", "0"))
	setenv("WALLABY_TEST_CLI_LOG", getenvString("WALLABY_TEST_CLI_LOG", "1"))
	setenv("WALLABY_TEST_S3_BUCKET", getenvString("WALLABY_TEST_S3_BUCKET", "wallaby-test"))
	setenv("WALLABY_TEST_S3_ACCESS_KEY", getenvString("WALLABY_TEST_S3_ACCESS_KEY", "wallaby"))
	setenv("WALLABY_TEST_S3_SECRET_KEY", getenvString("WALLABY_TEST_S3_SECRET_KEY", "wallabysecret"))
	setenv("WALLABY_TEST_S3_REGION", getenvString("WALLABY_TEST_S3_REGION", "us-east-1"))
	setenv("WALLABY_TEST_DUCKLAKE", getenvString("WALLABY_TEST_DUCKLAKE", "1"))
	setenv("TEST_CLICKHOUSE_HTTP_PORT", getenvString("TEST_CLICKHOUSE_HTTP_PORT", defaultClickHouseHTTPPort))
	setenv("WALLABY_TEST_GLUE_REGION", getenvString("WALLABY_TEST_GLUE_REGION", "us-east-1"))
	setenv("AWS_ACCESS_KEY_ID", getenvString("AWS_ACCESS_KEY_ID", "test"))
	setenv("AWS_SECRET_ACCESS_KEY", getenvString("AWS_SECRET_ACCESS_KEY", "test"))
	setenv("AWS_REGION", getenvString("AWS_REGION", getenvString("WALLABY_TEST_GLUE_REGION", "us-east-1")))
	setenv("AWS_DEFAULT_REGION", getenvString("AWS_DEFAULT_REGION", getenvString("WALLABY_TEST_GLUE_REGION", "us-east-1")))
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	if kubeconfig == "" {
		_ = os.Setenv("KUBECONFIG", "/dev/null")
	}
	_ = os.Setenv("AWS_PROFILE", "")
	_ = os.Setenv("AWS_DEFAULT_PROFILE", "")
	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	_ = os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	_ = os.Setenv("AWS_SDK_LOAD_CONFIG", "0")

	_ = os.Unsetenv("WALLABY_TEST_SNOWFLAKE_DSN")
	_ = os.Unsetenv("WALLABY_TEST_SNOWFLAKE_SCHEMA")
}

func defaultTestPostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		defaultPostgresUser,
		defaultPostgresPassword,
		defaultPostgresLocalBindHost,
		activePostgresLocalPort,
		defaultPostgresDatabase,
	)
}

func defaultClickHouseDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s", defaultClickHouseUser, defaultClickHousePassword, defaultPostgresLocalBindHost, activeClickHouseLocalPort, defaultClickHouseDatabase)
}

func defaultBrokers() string {
	return fmt.Sprintf("%s:%s", defaultPostgresLocalBindHost, activeKafkaLocalPort)
}

func localBrokers() string {
	return defaultBrokers()
}

func localURL(host, port string) string {
	return fmt.Sprintf("http://%s:%s", host, port)
}

func (h *integrationHarness) startPostgresPortForward(namespace, localPort string) (string, error) {
	h.logf("starting postgres port-forward in namespace %s", namespace)
	resolvedLocalPort, stop, err := h.startLocalPortForward(namespace, defaultPostgresName, localPort, defaultPostgresServicePort)
	if err != nil {
		return "", err
	}
	h.pgPortForwardStop = stop
	return resolvedLocalPort, nil
}

func (h *integrationHarness) startLocalPortForward(namespace, name, localPort, servicePort string) (string, context.CancelFunc, error) {
	resolvedLocalPort := localPort

	if !isPortAvailable(defaultPostgresLocalBindHost, resolvedLocalPort) {
		return "", nil, fmt.Errorf("local %s:%s is already in use for %s", defaultPostgresLocalBindHost, resolvedLocalPort, name)
	}

	podName, err := h.pickPodForService(namespace, name)
	if err != nil {
		return "", nil, err
	}

	stop, err := h.startPodPortForward(namespace, podName, resolvedLocalPort, servicePort)
	if err != nil {
		return "", nil, fmt.Errorf("start %s port-forward: %w", name, err)
	}

	localAddress := net.JoinHostPort(defaultPostgresLocalBindHost, resolvedLocalPort)
	if err := waitForLocalPort(localAddress, 45*time.Second); err != nil {
		stop()
		waitForPortRelease(defaultPostgresLocalBindHost, resolvedLocalPort, 5*time.Second)
		return "", nil, err
	}

	return resolvedLocalPort, stop, nil
}

func (h *integrationHarness) hasExistingManagedService(namespace, name string) bool {
	client, _, err := h.kubernetesClient()
	if err != nil {
		return false
	}
	_, svcErr := client.CoreV1().Services(namespace).Get(context.Background(), name, metav1.GetOptions{})
	_, depErr := client.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
	return svcErr == nil && depErr == nil
}

func isPortAvailable(host, port string) bool {
	addr := net.JoinHostPort(host, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func waitForPortRelease(host, port string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		if isPortAvailable(host, port) {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitForLocalPort(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waiting for local port %q: %w", address, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func postgresKindManifest(namespace string) (string, error) {
	pgPort := mustInt32(defaultPostgresServicePort)
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)

	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultPostgresName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultPostgresName),
			Ports: []corev1.ServicePort{
				{
					Name:       "pg",
					Protocol:   corev1.ProtocolTCP,
					Port:       pgPort,
					TargetPort: intstr.FromInt(int(pgPort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultPostgresName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultPostgresName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultPostgresName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "postgres",
							Image:           defaultPostgresImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "POSTGRES_USER", Value: defaultPostgresUser},
								{Name: "POSTGRES_PASSWORD", Value: defaultPostgresPassword},
								{Name: "POSTGRES_DB", Value: defaultPostgresDatabase},
							},
							Args: []string{
								"-c", "wal_level=logical",
								"-c", "max_wal_senders=10",
								"-c", "max_replication_slots=10",
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: pgPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"pg_isready", "-U", defaultPostgresUser, "-d", defaultPostgresDatabase},
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}

	return asManifestYAML(&service, &deployment)
}

func clickhouseKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultClickHouseName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultClickHouseName),
			Ports: []corev1.ServicePort{
				{
					Name:       "native",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultClickHouseServicePort),
					TargetPort: intstr.FromInt(mustInt(defaultClickHouseServicePort)),
				},
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultClickHouseHTTPPort),
					TargetPort: intstr.FromInt(mustInt(defaultClickHouseHTTPPort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultClickHouseName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultClickHouseName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultClickHouseName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "clickhouse",
							Image:           defaultClickHouseImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "CLICKHOUSE_DB", Value: defaultClickHouseDatabase},
								{Name: "CLICKHOUSE_USER", Value: defaultClickHouseUser},
								{Name: "CLICKHOUSE_PASSWORD", Value: defaultClickHousePassword},
								{Name: "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", Value: "1"},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "native",
									ContainerPort: mustInt32(defaultClickHouseServicePort),
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "http",
									ContainerPort: mustInt32(defaultClickHouseHTTPPort),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("native"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func minioKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultMinioName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultMinioName),
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultMinioServicePort),
					TargetPort: intstr.FromInt(mustInt(defaultMinioServicePort)),
				},
				{
					Name:       "console",
					Protocol:   corev1.ProtocolTCP,
					Port:       9003,
					TargetPort: intstr.FromInt(9001),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultMinioName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultMinioName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultMinioName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "minio",
							Image:           defaultMinioImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "MINIO_ROOT_USER", Value: defaultMinioUser},
								{Name: "MINIO_ROOT_PASSWORD", Value: defaultMinioSecret},
							},
							Args: []string{"server", "/data", "--console-address", ":9001"},
							Ports: []corev1.ContainerPort{
								{Name: "api", ContainerPort: 9000, Protocol: corev1.ProtocolTCP},
								{Name: "console", ContainerPort: 9001, Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("api"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func kafkaKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultKafkaName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultKafkaName),
			Ports: []corev1.ServicePort{
				{
					Name:       "kafka",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultKafkaServicePort),
					TargetPort: intstr.FromInt(mustInt(defaultKafkaServicePort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultKafkaName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultKafkaName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultKafkaName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "redpanda",
							Image:           defaultKafkaImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args: []string{
								"redpanda",
								"start",
								"--overprovisioned",
								"--smp",
								"1",
								"--memory",
								"1G",
								"--reserve-memory",
								"0M",
								"--node-id",
								"0",
								"--check=false",
								"--kafka-addr",
								"PLAINTEXT://0.0.0.0:9092",
								"--advertise-kafka-addr",
								"PLAINTEXT://localhost:9094",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "kafka",
									ContainerPort: mustInt32(defaultKafkaServicePort),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("kafka"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func localStackKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultLocalStackName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultLocalStackName),
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultLocalStackLocalPort),
					TargetPort: intstr.FromInt(mustInt(defaultLocalStackLocalPort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultLocalStackName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultLocalStackName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultLocalStackName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "localstack",
							Image:           defaultLocalStackImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "SERVICES", Value: "glue,sts"},
								{Name: "DEFAULT_REGION", Value: defaultLocalStackRegion},
								{Name: "AWS_ACCESS_KEY_ID", Value: "test"},
								{Name: "AWS_SECRET_ACCESS_KEY", Value: "test"},
							},
							Ports: []corev1.ContainerPort{
								{Name: "api", ContainerPort: mustInt32(defaultLocalStackLocalPort), Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("api"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func httpTestKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultHTTPTestName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultHTTPTestName),
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultHTTPTestServicePort),
					TargetPort: intstr.FromInt(mustInt(defaultHTTPTestServicePort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultHTTPTestName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultHTTPTestName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultHTTPTestName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "http-test",
							Image:           defaultHTTPTestImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{"/bin/sh", "-lc", fmt.Sprintf(`
cat <<'PY' >/tmp/http_test_server.py
import base64
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

LAST_CAPTURE = {
    "method": "",
    "path": "",
    "headers": {},
    "body_base64": "",
}


class CaptureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, payload, status):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path != "/last":
            self._json({"error": "not found"}, 404)
            return
        self._json(LAST_CAPTURE, 200)

    def do_POST(self):
        if self.path != "/capture":
            self._json({"error": "not found"}, 404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
        LAST_CAPTURE["method"] = self.command
        LAST_CAPTURE["path"] = self.path
        LAST_CAPTURE["headers"] = dict(self.headers.items())
        LAST_CAPTURE["body_base64"] = base64.b64encode(body).decode("utf-8")
        self._json({"ok": True}, 200)


if __name__ == "__main__":
    server = HTTPServer(("", %s), CaptureHandler)
    server.serve_forever()
PY
python3 /tmp/http_test_server.py`, defaultHTTPTestServicePort)},
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: mustInt32(defaultHTTPTestServicePort), Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("http"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func fakesnowKindManifest(namespace string) (string, error) {
	probeTimeoutSeconds := int32(5)
	replicas := int32(1)
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: manifestMetadata(defaultFakesnowName, namespace),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: manifestSelector(defaultFakesnowName),
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       mustInt32(defaultFakesnowServicePort),
					TargetPort: intstr.FromInt(mustInt(defaultFakesnowServicePort)),
				},
			},
		},
	}

	deployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: manifestMetadata(defaultFakesnowName, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: manifestSelector(defaultFakesnowName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: manifestSelector(defaultFakesnowName),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "fakesnow",
							Image:           defaultFakesnowImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "FAKESNOW_HOST", Value: "0.0.0.0"},
								{Name: "FAKESNOW_PORT", Value: defaultFakesnowServicePort},
							},
							Command: []string{"/bin/sh", "-lc", fmt.Sprintf(`
cat <<'PY' >/tmp/fakesnow_entrypoint.py
import inspect
import os

import uvicorn


def _load_server_func():
    try:
        import fakesnow
    except Exception:
        return None

    server_fn = getattr(fakesnow, "server", None)
    if server_fn is not None:
        return server_fn

    try:
        from fakesnow import server as server_mod  # type: ignore
    except Exception:
        return None

    return getattr(server_mod, "server", None)


def _load_asgi_app():
    candidates = [
        "fakesnow.server",
        "fakesnow.app",
        "fakesnow.api",
        "fakesnow.http",
        "fakesnow.main",
    ]
    for name in candidates:
        try:
            module = __import__(name, fromlist=["app"])
        except Exception:
            continue
        app = getattr(module, "app", None)
        if app is not None:
            return app
        create_app = getattr(module, "create_app", None)
        if create_app is not None:
            return create_app()
    return None


def main():
    host = os.getenv("FAKESNOW_HOST", "0.0.0.0")
    port = int(os.getenv("FAKESNOW_PORT", "8000"))

    app = _load_asgi_app()
    if app is None:
        raise RuntimeError("Unable to locate fakesnow ASGI app")

    print(f"fakesnow uvicorn starting on {host}:{port}", flush=True)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
PY
python3 -m pip install --no-cache-dir "fakesnow[server]==%s"
python3 /tmp/fakesnow_entrypoint.py`, defaultFakesnowVersion)},
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: mustInt32(defaultFakesnowServicePort), Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromString("http"),
									},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								TimeoutSeconds:      probeTimeoutSeconds,
								FailureThreshold:    30,
							},
						},
					},
				},
			},
		},
	}
	return asManifestYAML(&service, &deployment)
}

func asManifestYAML(objs ...interface{}) (string, error) {
	var b strings.Builder
	for i, obj := range objs {
		data, err := yaml.Marshal(obj)
		if err != nil {
			return "", err
		}
		if i > 0 {
			b.WriteString("---\n")
		}
		b.Write(data)
		if len(data) > 0 && data[len(data)-1] != '\n' {
			b.WriteByte('\n')
		}
	}
	return b.String(), nil
}

func mustInt(raw string) int {
	parsed, _ := strconv.Atoi(raw)
	return parsed
}

func mustInt32(raw string) int32 {
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0
	}
	return int32(parsed)
}

func manifestMetadata(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
		Labels: map[string]string{
			"app": name,
		},
	}
}

func manifestSelector(name string) map[string]string {
	return map[string]string{
		"app": name,
	}
}

func commandOutput(ctx context.Context, dir string, name string, args ...string) ([]byte, error) { //nolint:unparam
	if harnessVerbose() {
		fmt.Fprintf(os.Stderr, "$ %s %s\n", name, strings.Join(args, " "))
	}
	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if harnessVerbose() {
		cmd.Stdout = io.MultiWriter(os.Stdout, &output)
		cmd.Stderr = io.MultiWriter(os.Stderr, &output)
	} else {
		cmd.Stdout = &output
		cmd.Stderr = &output
	}
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return output.Bytes(), nil
}

func (h *integrationHarness) logf(format string, args ...any) {
	if !harnessVerbose() {
		return
	}
	fmt.Fprintf(os.Stderr, "[it] "+format+"\n", args...)
}

func harnessVerbose() bool {
	if testing.Verbose() {
		return true
	}

	for _, raw := range []string{
		os.Getenv("IT_VERBOSE"),
		os.Getenv("WALLABY_IT_VERBOSE"),
		os.Getenv("GO_TEST_VERBOSE"),
	} {
		if isTruthy(raw) {
			return true
		}
	}
	return false
}

func isTruthy(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on", "t", "y":
		return true
	default:
		return false
	}
}

func kindHasCluster(rawClusters, name string) bool {
	for _, cluster := range strings.Split(strings.TrimSpace(rawClusters), "\n") {
		if strings.TrimSpace(cluster) == name {
			return true
		}
	}
	return false
}

func getenvString(name, fallback string) string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	return raw
}

func setenv(name, value string) {
	if strings.TrimSpace(os.Getenv(name)) == "" {
		_ = os.Setenv(name, value)
	}
}

func defaultK8sNamespace() string {
	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if namespace == "" {
		return defaultKindNamespace
	}
	return namespace
}
