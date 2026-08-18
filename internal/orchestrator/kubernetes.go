package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/josephjohncox/wallaby/internal/workflow"
)

const (
	serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

	flowIDMetadataKey      = "wallaby.flow-id"
	flowHashMetadataKey    = "wallaby.flow-hash"
	generationMetadataKey  = "wallaby.generation"
	backendMetadataKey     = "wallaby.backend"
	executionIDMetadataKey = "wallaby.execution-id"
	kubernetesBackend      = "kubernetes"
)

// KubernetesConfig configures the Kubernetes job dispatcher.
type KubernetesConfig struct {
	KubeconfigPath                  string
	KubeContext                     string
	APIServer                       string
	BearerToken                     string
	CAFile                          string
	CAData                          string
	ClientCertFile                  string
	ClientKeyFile                   string
	InsecureSkipTLS                 bool
	Namespace                       string
	JobImage                        string
	JobImagePullPolicy              string
	JobServiceAccount               string
	JobAutomountServiceAccountToken bool
	JobNamePrefix                   string
	JobTTLSeconds                   int
	JobBackoffLimit                 int
	MaxEmptyReads                   int
	JobLabels                       map[string]string
	JobAnnotations                  map[string]string
	JobCommand                      []string
	JobArgs                         []string
	JobEnv                          map[string]string
	JobEnvFrom                      []string
	SnowflakeEnabled                bool
	SnowflakeAccount                string
	SnowflakeUser                   string
	SnowflakeHost                   string
	SnowflakePrivateKeyFile         string
	SnowflakePrivateKeySecretName   string
	SnowflakePrivateKeySecretKey    string
}

// KubernetesDispatcher triggers flow workers as Kubernetes Jobs.
type KubernetesDispatcher struct {
	client    kubernetes.Interface
	namespace string
	cfg       KubernetesConfig
}

// NewKubernetesDispatcher builds a dispatcher using in-cluster or kubeconfig credentials.
func NewKubernetesDispatcher(ctx context.Context, cfg KubernetesConfig) (*KubernetesDispatcher, error) {
	if cfg.JobImage == "" {
		return nil, errors.New("kubernetes job image is required")
	}
	if cfg.SnowflakeEnabled {
		for name, value := range map[string]string{
			"account": cfg.SnowflakeAccount, "user": cfg.SnowflakeUser, "host": cfg.SnowflakeHost,
			"private key file": cfg.SnowflakePrivateKeyFile, "private key secret name": cfg.SnowflakePrivateKeySecretName,
			"private key secret key": cfg.SnowflakePrivateKeySecretKey,
		} {
			if strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("kubernetes Snowflake %s is required", name)
			}
		}
	}

	client, namespace, err := resolveKubeClient(cfg)
	if err != nil {
		return nil, err
	}

	if namespace == "" {
		namespace = "default"
	}

	if cfg.JobImagePullPolicy == "" {
		cfg.JobImagePullPolicy = "IfNotPresent"
	}
	if cfg.JobNamePrefix == "" {
		cfg.JobNamePrefix = "wallaby-worker"
	}

	return &KubernetesDispatcher{
		client:    client,
		namespace: namespace,
		cfg:       cfg,
	}, nil
}

// EnqueueGeneration creates an idempotent Job identity for (flow,generation).
func (k *KubernetesDispatcher) EnqueueGeneration(ctx context.Context, flowID string, generation int64) error {
	if flowID == "" || generation <= 0 {
		return errors.New("flow id and positive generation are required")
	}
	return k.enqueue(ctx, flowID, generation, buildGenerationJobName(k.cfg.JobNamePrefix, flowID, generation))
}

// EnqueueRunOnce schedules one uniquely identified Job against the lifecycle
// generation captured by the caller.
func (k *KubernetesDispatcher) EnqueueRunOnce(ctx context.Context, flowID string, generation int64) error {
	if flowID == "" || generation <= 0 {
		return errors.New("flow id and positive generation are required")
	}
	jobName := buildRunOnceJobName(k.cfg.JobNamePrefix, flowID, generation, uuid.NewString())
	return k.enqueue(ctx, flowID, generation, jobName)
}

func (k *KubernetesDispatcher) enqueue(ctx context.Context, flowID string, generation int64, jobName string) error {
	if err := k.createJob(ctx, flowID, jobName, generation); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing, err := k.client.BatchV1().Jobs(k.namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing job: %w", err)
	}
	existingGeneration, executionID, err := validateJobOwnership(existing, flowID)
	if err != nil || existingGeneration != generation || executionID != jobName {
		if err == nil {
			err = fmt.Errorf("unexpected generation %d or execution id %q", existingGeneration, executionID)
		}
		return fmt.Errorf("existing kubernetes job %q has non-authoritative ownership: %w", jobName, err)
	}
	// An existing generation Job is durable proof that lifecycle dispatch
	// happened. Run-once Jobs use unique names and do not share this path.
	return nil
}

// CancelThroughGeneration foreground-deletes every matching Job through the
// requested generation and returns terminal proof only after all are absent.
func (k *KubernetesDispatcher) CancelThroughGeneration(ctx context.Context, flowID string, generation int64) (workflow.CancellationReceipt, error) {
	receipt := workflow.CancellationReceipt{ThroughGeneration: generation, Backend: kubernetesBackend}
	if flowID == "" || generation <= 0 {
		return receipt, errors.New("flow id and positive generation are required")
	}
	jobs, err := k.client.BatchV1().Jobs(k.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return receipt, fmt.Errorf("list kubernetes jobs: %w", err)
	}
	terminalIDs := map[string]struct{}{
		generationExecutionID(k.jobNamePrefix(), flowID, generation): {},
		buildJobName(k.jobNamePrefix(), flowID):                      {},
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.Annotations[flowIDMetadataKey] != flowID && job.Labels[flowHashMetadataKey] != flowLabelValue(flowID) {
			continue
		}
		jobGeneration, executionID, metadataErr := validateJobOwnership(job, flowID)
		if metadataErr != nil {
			return receipt, metadataErr
		}
		if jobGeneration > generation {
			continue
		}
		if err := k.deleteJob(ctx, job.Name); err != nil {
			return receipt, err
		}
		if err := k.waitForJobDeletion(ctx, job.Name, 30*time.Second); err != nil {
			return receipt, err
		}
		terminalIDs[executionID] = struct{}{}
	}
	receipt.TerminalExecutionIDs = make([]string, 0, len(terminalIDs))
	for executionID := range terminalIDs {
		receipt.TerminalExecutionIDs = append(receipt.TerminalExecutionIDs, executionID)
	}
	sort.Strings(receipt.TerminalExecutionIDs)
	receipt.Terminal = true
	return receipt, nil
}
func (k *KubernetesDispatcher) CancelFlow(ctx context.Context, flowID string) error {
	if flowID == "" {
		return errors.New("flow id is required")
	}
	jobName := buildJobName(k.cfg.JobNamePrefix, flowID)
	if err := k.deleteJob(ctx, jobName); err != nil {
		return err
	}
	return k.waitForJobDeletion(ctx, jobName, 30*time.Second)
}

func (k *KubernetesDispatcher) createJob(ctx context.Context, flowID, jobName string, generation int64) error {
	executionID := jobName
	generationText := strconv.FormatInt(generation, 10)
	// User metadata is applied first. The authoritative ownership keys are
	// always written last and therefore cannot be overridden.
	labels := mergeLabels(k.cfg.JobLabels, map[string]string{
		"app.kubernetes.io/name":      "wallaby-worker",
		"app.kubernetes.io/component": "worker",
		flowHashMetadataKey:           flowLabelValue(flowID),
		generationMetadataKey:         generationText,
		backendMetadataKey:            kubernetesBackend,
		executionIDMetadataKey:        executionID,
	})
	annotations := mergeLabels(k.cfg.JobAnnotations, map[string]string{
		flowIDMetadataKey:      flowID,
		generationMetadataKey:  generationText,
		backendMetadataKey:     kubernetesBackend,
		executionIDMetadataKey: executionID,
	})

	command := k.cfg.JobCommand
	if len(command) == 0 {
		command = []string{"/usr/local/bin/wallaby-worker"}
	}
	args := authoritativeWorkerArgsWithSnowflake(k.cfg.JobArgs, flowID, generation, executionID, k.cfg.MaxEmptyReads, k.cfg.SnowflakeEnabled, k.cfg.SnowflakeAccount, k.cfg.SnowflakeUser, k.cfg.SnowflakeHost, k.cfg.SnowflakePrivateKeyFile)

	env := mapToEnvVars(k.cfg.JobEnv)
	envFrom := parseEnvFrom(k.cfg.JobEnvFrom)

	container := corev1.Container{
		Name: "worker", Image: k.cfg.JobImage, ImagePullPolicy: corev1.PullPolicy(k.cfg.JobImagePullPolicy),
		Command: command, Args: args, Env: env, EnvFrom: envFrom,
	}
	var volumes []corev1.Volume
	if k.cfg.SnowflakeEnabled {
		const keyVolume = "snowflake-private-key"
		const keyItemPath = "private-key.pem"
		mode := int32(0o400)
		volumes = []corev1.Volume{{
			Name: keyVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  k.cfg.SnowflakePrivateKeySecretName,
				Items:       []corev1.KeyToPath{{Key: k.cfg.SnowflakePrivateKeySecretKey, Path: keyItemPath, Mode: &mode}},
				DefaultMode: &mode,
			}},
		}}
		container.VolumeMounts = []corev1.VolumeMount{{Name: keyVolume, MountPath: k.cfg.SnowflakePrivateKeyFile, SubPath: keyItemPath, ReadOnly: true}}
	}

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "batch/v1",
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   k.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: optionalInt32(k.cfg.JobTTLSeconds),
			BackoffLimit:            optionalInt32(k.cfg.JobBackoffLimit),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName:           k.cfg.JobServiceAccount,
					AutomountServiceAccountToken: boolPtr(k.cfg.JobAutomountServiceAccountToken),
					RestartPolicy:                corev1.RestartPolicyNever,
					Volumes:                      volumes,
					Containers:                   []corev1.Container{container},
				},
			},
		},
	}

	_, err := k.client.BatchV1().Jobs(k.namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create kubernetes job: %w", err)
	}

	return nil
}

func resolveKubeClient(cfg KubernetesConfig) (kubernetes.Interface, string, error) {
	var (
		restCfg   *rest.Config
		namespace string
		err       error
	)

	switch {
	case hasStaticConfig(cfg):
		restCfg, namespace, err = configFromStatic(cfg)
	case cfg.KubeconfigPath != "":
		restCfg, namespace, err = configFromKubeconfig(cfg)
	default:
		restCfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, "", err
		}
		namespace = strings.TrimSpace(cfg.Namespace)
		if namespace == "" {
			if ns, nsErr := readNamespace(); nsErr == nil {
				namespace = ns
			}
		}
	}
	if err != nil {
		return nil, "", err
	}

	if restCfg.Timeout == 0 {
		restCfg.Timeout = 15 * time.Second
	}
	if namespace == "" {
		namespace = strings.TrimSpace(cfg.Namespace)
	}
	if namespace == "" {
		namespace = "default"
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, "", fmt.Errorf("create kubernetes client: %w", err)
	}
	return client, namespace, nil
}

func hasStaticConfig(cfg KubernetesConfig) bool {
	return cfg.APIServer != "" || cfg.BearerToken != "" || cfg.CAFile != "" || cfg.CAData != "" || cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" || cfg.InsecureSkipTLS
}

func configFromStatic(cfg KubernetesConfig) (*rest.Config, string, error) {
	if cfg.APIServer == "" {
		return nil, "", errors.New("kubernetes api server is required for out-of-cluster config")
	}

	caData := decodeMaybeBase64(cfg.CAData, true)

	restCfg := &rest.Config{
		Host:        normalizeServerURL(cfg.APIServer),
		BearerToken: strings.TrimSpace(cfg.BearerToken),
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: cfg.InsecureSkipTLS,
			CAFile:   strings.TrimSpace(cfg.CAFile),
			CAData:   caData,
			CertFile: strings.TrimSpace(cfg.ClientCertFile),
			KeyFile:  strings.TrimSpace(cfg.ClientKeyFile),
		},
	}

	namespace := strings.TrimSpace(cfg.Namespace)
	return restCfg, namespace, nil
}

func configFromKubeconfig(cfg KubernetesConfig) (*rest.Config, string, error) {
	loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: cfg.KubeconfigPath}
	overrides := &clientcmd.ConfigOverrides{}
	if strings.TrimSpace(cfg.KubeContext) != "" {
		overrides.CurrentContext = strings.TrimSpace(cfg.KubeContext)
	}
	if strings.TrimSpace(cfg.Namespace) != "" {
		overrides.Context.Namespace = strings.TrimSpace(cfg.Namespace)
	}

	clientCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restCfg, err := clientCfg.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}
	namespace, _, err := clientCfg.Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig namespace: %w", err)
	}
	return restCfg, namespace, nil
}

func decodeMaybeBase64(value string, base64Decode bool) []byte {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.Contains(value, "BEGIN CERTIFICATE") || strings.Contains(value, "BEGIN RSA PRIVATE KEY") || strings.Contains(value, "BEGIN PRIVATE KEY") {
		return []byte(value)
	}
	if base64Decode {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err == nil {
			return decoded
		}
	}
	return []byte(value)
}

func normalizeServerURL(server string) string {
	trim := strings.TrimSpace(server)
	if trim == "" {
		return ""
	}
	if strings.HasPrefix(trim, "http://") || strings.HasPrefix(trim, "https://") {
		return trim
	}
	return "https://" + trim
}

func readNamespace() (string, error) {
	path := filepath.Join(serviceAccountPath, "namespace")
	// #nosec G304 -- path is fixed within the service account mount.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

func buildJobName(prefix, flowID string) string {
	base := sanitizeName(prefix + "-" + flowID)
	if base == "" {
		base = "flow"
	}
	suffix := jobNameSuffix(prefix, flowID)
	maxBase := 63 - len(suffix) - 1
	if maxBase < 1 {
		maxBase = 1
	}
	if maxBase < len(base) {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	if base == "" {
		base = "flow"
	}
	return base + "-" + suffix
}

func buildGenerationJobName(prefix, flowID string, generation int64) string {
	return buildJobName(prefix, fmt.Sprintf("%s@generation:%d", flowID, generation))
}

func buildRunOnceJobName(prefix, flowID string, generation int64, attemptID string) string {
	return buildJobName(prefix, fmt.Sprintf("%s@generation:%d@run:%s", flowID, generation, attemptID))
}

func generationExecutionID(prefix, flowID string, generation int64) string {
	return buildGenerationJobName(prefix, flowID, generation)
}

func (k *KubernetesDispatcher) jobNamePrefix() string {
	if strings.TrimSpace(k.cfg.JobNamePrefix) == "" {
		return "wallaby-worker"
	}
	return k.cfg.JobNamePrefix
}

func validateJobOwnership(job *batchv1.Job, flowID string) (int64, string, error) {
	if job == nil {
		return 0, "", errors.New("malformed kubernetes ownership metadata: nil job")
	}
	annotations := job.Annotations
	labels := job.Labels
	generationText := annotations[generationMetadataKey]
	generation, err := strconv.ParseInt(generationText, 10, 64)
	if annotations[flowIDMetadataKey] != flowID || err != nil || generation < 0 ||
		annotations[backendMetadataKey] != kubernetesBackend || annotations[executionIDMetadataKey] == "" ||
		annotations[executionIDMetadataKey] != job.Name || labels[flowHashMetadataKey] != flowLabelValue(flowID) ||
		labels[generationMetadataKey] != generationText || labels[backendMetadataKey] != kubernetesBackend ||
		labels[executionIDMetadataKey] != annotations[executionIDMetadataKey] {
		return 0, "", fmt.Errorf("malformed kubernetes ownership metadata for job %q", job.Name)
	}
	return generation, annotations[executionIDMetadataKey], nil
}

func flowLabelValue(flowID string) string {
	hash := sha256.Sum256([]byte(flowID))
	return hex.EncodeToString(hash[:8])
}

func jobNameSuffix(prefix, flowID string) string {
	seed := strings.TrimSpace(prefix) + "|" + strings.TrimSpace(flowID)
	hash := sha256.Sum256([]byte(seed))
	// 16 hex characters (64 bits) keeps collisions extremely unlikely and stays readable.
	return hex.EncodeToString(hash[:8])
}

func (k *KubernetesDispatcher) deleteJob(ctx context.Context, name string) error {
	policy := metav1.DeletePropagationForeground
	if err := k.client.BatchV1().Jobs(k.namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

func (k *KubernetesDispatcher) waitForJobDeletion(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job deletion: %s", name)
		}
		if _, err := k.client.BatchV1().Jobs(k.namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("check job deletion: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func sanitizeName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		switch {
		case isAlphaNum:
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '.' || r == '_':
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		default:
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	return out
}

func mergeLabels(base, override map[string]string) map[string]string {
	if base == nil && override == nil {
		return nil
	}
	out := make(map[string]string)
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func authoritativeWorkerArgsWithSnowflake(args []string, flowID string, generation int64, executionID string, maxEmpty int, snowflakeEnabled bool, account, user, host, privateKeyFile string) []string {
	reserved := map[string]struct{}{
		"flow-id":                    {},
		"generation":                 {},
		"execution-backend":          {},
		"execution-id":               {},
		"snowflake-enabled":          {},
		"snowflake-account":          {},
		"snowflake-user":             {},
		"snowflake-host":             {},
		"snowflake-private-key-file": {},
	}
	out := make([]string, 0, len(args)+8)
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			continue
		}
		name := strings.TrimPrefix(strings.SplitN(arg, "=", 2)[0], "--")
		if _, isReserved := reserved[name]; !isReserved || !strings.HasPrefix(arg, "--") {
			out = append(out, arg)
			continue
		}
		if !strings.Contains(arg, "=") && index+1 < len(args) && !strings.HasPrefix(args[index+1], "--") {
			index++
		}
	}
	out = append(out,
		"--flow-id", flowID,
		"--generation", strconv.FormatInt(generation, 10),
		"--execution-backend", kubernetesBackend,
		"--execution-id", executionID,
		"--snowflake-enabled="+strconv.FormatBool(snowflakeEnabled),
		"--snowflake-account", account,
		"--snowflake-user", user,
		"--snowflake-host", host,
		"--snowflake-private-key-file", privateKeyFile,
	)
	if maxEmpty > 0 && !hasFlag(out, "max-empty-reads") {
		out = append(out, "--max-empty-reads", strconv.Itoa(maxEmpty))
	}
	return out
}

func hasFlag(args []string, name string) bool {
	needle := "--" + name
	for _, arg := range args {
		if arg == needle || strings.HasPrefix(arg, needle+"=") {
			return true
		}
	}
	return false
}

func mapToEnvVars(values map[string]string) []corev1.EnvVar {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]corev1.EnvVar, 0, len(values))
	for _, key := range keys {
		out = append(out, corev1.EnvVar{Name: key, Value: values[key]})
	}
	return out
}

func parseEnvFrom(entries []string) []corev1.EnvFromSource {
	if len(entries) == 0 {
		return nil
	}
	out := make([]corev1.EnvFromSource, 0, len(entries))
	for _, entry := range entries {
		item := strings.TrimSpace(entry)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(parts[0]))
		name := strings.TrimSpace(parts[1])
		if name == "" {
			continue
		}
		switch kind {
		case "secret", "secretref":
			out = append(out, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}})
		case "configmap", "configmapref", "config-map":
			out = append(out, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}})
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func boolPtr(value bool) *bool { return &value }

func optionalInt32(value int) *int32 {
	if value <= 0 {
		return nil
	}
	if value > math.MaxInt32 {
		value = math.MaxInt32
	}
	// #nosec G115 -- value clamped to MaxInt32 above.
	val := int32(value)
	return &val
}
