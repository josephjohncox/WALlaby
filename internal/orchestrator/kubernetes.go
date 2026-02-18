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
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// KubernetesConfig configures the Kubernetes job dispatcher.
type KubernetesConfig struct {
	KubeconfigPath     string
	KubeContext        string
	APIServer          string
	BearerToken        string
	CAFile             string
	CAData             string
	ClientCertFile     string
	ClientKeyFile      string
	InsecureSkipTLS    bool
	Namespace          string
	JobImage           string
	JobImagePullPolicy string
	JobServiceAccount  string
	JobNamePrefix      string
	JobTTLSeconds      int
	JobBackoffLimit    int
	MaxEmptyReads      int
	JobLabels          map[string]string
	JobAnnotations     map[string]string
	JobCommand         []string
	JobArgs            []string
	JobEnv             map[string]string
	JobEnvFrom         []string
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

// EnqueueFlow creates a Job for the given flow.
func (k *KubernetesDispatcher) EnqueueFlow(ctx context.Context, flowID string) error {
	if flowID == "" {
		return errors.New("flow id is required")
	}

	jobName := buildJobName(k.cfg.JobNamePrefix, flowID)
	if err := k.createJob(ctx, flowID, jobName); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return err
	}

	existing, err := k.client.BatchV1().Jobs(k.namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing job: %w", err)
	}
	if jobActive(existing) {
		return nil
	}
	if jobFinished(existing) {
		if err := k.deleteJob(ctx, jobName); err != nil {
			return err
		}
		if err := k.waitForJobDeletion(ctx, jobName, 30*time.Second); err != nil {
			return err
		}
		if err := k.createJob(ctx, flowID, jobName); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (k *KubernetesDispatcher) createJob(ctx context.Context, flowID, jobName string) error {
	labels := mergeLabels(map[string]string{
		"app.kubernetes.io/name":      "wallaby-worker",
		"app.kubernetes.io/component": "worker",
		"wallaby.flow-id":             flowID,
	}, k.cfg.JobLabels)
	annotations := mergeLabels(map[string]string{
		"wallaby.flow-id": flowID,
	}, k.cfg.JobAnnotations)

	command := k.cfg.JobCommand
	if len(command) == 0 {
		command = []string{"/usr/local/bin/wallaby-worker"}
	}
	args := ensureFlowArgs(k.cfg.JobArgs, flowID, k.cfg.MaxEmptyReads)

	env := mapToEnvVars(k.cfg.JobEnv)
	envFrom := parseEnvFrom(k.cfg.JobEnvFrom)

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
					ServiceAccountName: k.cfg.JobServiceAccount,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           k.cfg.JobImage,
							ImagePullPolicy: corev1.PullPolicy(k.cfg.JobImagePullPolicy),
							Command:         command,
							Args:            args,
							Env:             env,
							EnvFrom:         envFrom,
						},
					},
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

func jobActive(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	return job.Status.Active > 0
}

func jobFinished(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
		return true
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
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
		return "", err
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

func jobNameSuffix(prefix, flowID string) string {
	seed := strings.TrimSpace(prefix) + "|" + strings.TrimSpace(flowID)
	hash := sha256.Sum256([]byte(seed))
	// 16 hex characters (64 bits) keeps collisions extremely unlikely and stays readable.
	return hex.EncodeToString(hash[:8])
}

func (k *KubernetesDispatcher) deleteJob(ctx context.Context, name string) error {
	policy := metav1.DeletePropagationBackground
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

func ensureFlowArgs(args []string, flowID string, maxEmpty int) []string {
	out := append([]string{}, args...)
	if !hasFlag(out, "flow-id") {
		out = append(out, fmt.Sprintf("--flow-id=%s", flowID))
	}
	if maxEmpty > 0 && !hasFlag(out, "max-empty-reads") {
		out = append(out, fmt.Sprintf("--max-empty-reads=%d", maxEmpty))
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
