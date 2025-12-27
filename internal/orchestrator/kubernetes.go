package orchestrator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
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
	client    *http.Client
	baseURL   string
	token     string
	namespace string
	cfg       KubernetesConfig
}

// NewKubernetesDispatcher builds a dispatcher that uses in-cluster config.
func NewKubernetesDispatcher(ctx context.Context, cfg KubernetesConfig) (*KubernetesDispatcher, error) {
	if cfg.JobImage == "" {
		return nil, errors.New("kubernetes job image is required")
	}

	baseURL, token, client, ns, err := resolveKubeClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = ns
	}
	if namespace == "" {
		namespace = "default"
	}

	if cfg.JobImagePullPolicy == "" {
		cfg.JobImagePullPolicy = "IfNotPresent"
	}
	if cfg.JobNamePrefix == "" {
		cfg.JobNamePrefix = "ductstream-worker"
	}

	return &KubernetesDispatcher{
		client:    client,
		baseURL:   baseURL,
		token:     token,
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
	labels := mergeLabels(map[string]string{
		"app.kubernetes.io/name":      "ductstream-worker",
		"app.kubernetes.io/component": "worker",
		"ductstream.flow-id":          flowID,
	}, k.cfg.JobLabels)
	annotations := mergeLabels(nil, k.cfg.JobAnnotations)

	command := k.cfg.JobCommand
	if len(command) == 0 {
		command = []string{"/usr/local/bin/ductstream-worker"}
	}
	args := ensureFlowArgs(k.cfg.JobArgs, flowID, k.cfg.MaxEmptyReads)

	env := mapToEnvVars(k.cfg.JobEnv)
	envFrom := parseEnvFrom(k.cfg.JobEnvFrom)

	job := jobSpec{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: objectMeta{
			Name:        jobName,
			Namespace:   k.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: jobSpecDetails{
			TTLSecondsAfterFinished: optionalInt32(k.cfg.JobTTLSeconds),
			BackoffLimit:            optionalInt32(k.cfg.JobBackoffLimit),
			Template: podTemplate{
				Metadata: objectMeta{Labels: labels},
				Spec: podSpec{
					ServiceAccountName: k.cfg.JobServiceAccount,
					RestartPolicy:      "Never",
					Containers: []containerSpec{
						{
							Name:            "worker",
							Image:           k.cfg.JobImage,
							ImagePullPolicy: k.cfg.JobImagePullPolicy,
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

	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal kubernetes job: %w", err)
	}

	endpoint := fmt.Sprintf("%s/apis/batch/v1/namespaces/%s/jobs", k.baseURL, k.namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if k.token != "" {
		req.Header.Set("Authorization", "Bearer "+k.token)
	}

	resp, err := k.client.Do(req)
	if err != nil {
		return fmt.Errorf("create kubernetes job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("kubernetes job create failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

type jobSpec struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   objectMeta     `json:"metadata"`
	Spec       jobSpecDetails `json:"spec"`
}

type jobSpecDetails struct {
	TTLSecondsAfterFinished *int32      `json:"ttlSecondsAfterFinished,omitempty"`
	BackoffLimit            *int32      `json:"backoffLimit,omitempty"`
	Template                podTemplate `json:"template"`
}

type podTemplate struct {
	Metadata objectMeta `json:"metadata,omitempty"`
	Spec     podSpec    `json:"spec"`
}

type podSpec struct {
	ServiceAccountName string          `json:"serviceAccountName,omitempty"`
	RestartPolicy      string          `json:"restartPolicy,omitempty"`
	Containers         []containerSpec `json:"containers"`
}

type containerSpec struct {
	Name            string          `json:"name"`
	Image           string          `json:"image"`
	ImagePullPolicy string          `json:"imagePullPolicy,omitempty"`
	Command         []string        `json:"command,omitempty"`
	Args            []string        `json:"args,omitempty"`
	Env             []envVar        `json:"env,omitempty"`
	EnvFrom         []envFromSource `json:"envFrom,omitempty"`
}

type envVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type envFromSource struct {
	ConfigMapRef *nameRef `json:"configMapRef,omitempty"`
	SecretRef    *nameRef `json:"secretRef,omitempty"`
}

type nameRef struct {
	Name string `json:"name"`
}

type objectMeta struct {
	Name        string            `json:"name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type kubeConfig struct {
	CurrentContext string             `yaml:"current-context"`
	Clusters       []kubeClusterEntry `yaml:"clusters"`
	Users          []kubeUserEntry    `yaml:"users"`
	Contexts       []kubeContextEntry `yaml:"contexts"`
}

type kubeClusterEntry struct {
	Name    string      `yaml:"name"`
	Cluster kubeCluster `yaml:"cluster"`
}

type kubeCluster struct {
	Server                   string `yaml:"server"`
	CertificateAuthority     string `yaml:"certificate-authority"`
	CertificateAuthorityData string `yaml:"certificate-authority-data"`
	InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
}

type kubeUserEntry struct {
	Name string   `yaml:"name"`
	User kubeUser `yaml:"user"`
}

type kubeUser struct {
	Token                 string `yaml:"token"`
	ClientCertificate     string `yaml:"client-certificate"`
	ClientCertificateData string `yaml:"client-certificate-data"`
	ClientKey             string `yaml:"client-key"`
	ClientKeyData         string `yaml:"client-key-data"`
}

type kubeContextEntry struct {
	Name    string      `yaml:"name"`
	Context kubeContext `yaml:"context"`
}

type kubeContext struct {
	Cluster   string `yaml:"cluster"`
	User      string `yaml:"user"`
	Namespace string `yaml:"namespace"`
}

func resolveKubeClient(ctx context.Context, cfg KubernetesConfig) (string, string, *http.Client, string, error) {
	if cfg.APIServer != "" || cfg.BearerToken != "" || cfg.CAFile != "" || cfg.CAData != "" || cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" || cfg.InsecureSkipTLS {
		return clientFromStaticConfig(cfg)
	}
	if cfg.KubeconfigPath != "" {
		return clientFromKubeconfig(cfg)
	}
	baseURL, token, client, err := inClusterClient(ctx)
	if err != nil {
		return "", "", nil, "", err
	}
	ns, _ := readNamespace()
	return baseURL, token, client, ns, nil
}

func clientFromStaticConfig(cfg KubernetesConfig) (string, string, *http.Client, string, error) {
	if cfg.APIServer == "" {
		return "", "", nil, "", errors.New("kubernetes api server is required for out-of-cluster config")
	}
	tlsConfig, err := buildTLSConfig(tlsConfigInput{
		CAData:          cfg.CAData,
		CAFile:          cfg.CAFile,
		InsecureSkipTLS: cfg.InsecureSkipTLS,
		ClientCertFile:  cfg.ClientCertFile,
		ClientKeyFile:   cfg.ClientKeyFile,
	})
	if err != nil {
		return "", "", nil, "", err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}, Timeout: 15 * time.Second}
	return normalizeServerURL(cfg.APIServer), cfg.BearerToken, client, cfg.Namespace, nil
}

func clientFromKubeconfig(cfg KubernetesConfig) (string, string, *http.Client, string, error) {
	data, err := os.ReadFile(cfg.KubeconfigPath)
	if err != nil {
		return "", "", nil, "", fmt.Errorf("read kubeconfig: %w", err)
	}
	var kc kubeConfig
	if err := yaml.Unmarshal(data, &kc); err != nil {
		return "", "", nil, "", fmt.Errorf("parse kubeconfig: %w", err)
	}

	contextName := strings.TrimSpace(cfg.KubeContext)
	if contextName == "" {
		contextName = strings.TrimSpace(kc.CurrentContext)
	}
	if contextName == "" && len(kc.Contexts) > 0 {
		contextName = kc.Contexts[0].Name
	}
	if contextName == "" {
		return "", "", nil, "", errors.New("kubeconfig context not found")
	}

	ctxEntry, ok := findContext(kc.Contexts, contextName)
	if !ok {
		return "", "", nil, "", fmt.Errorf("kubeconfig context %q not found", contextName)
	}
	clusterEntry, ok := findCluster(kc.Clusters, ctxEntry.Context.Cluster)
	if !ok {
		return "", "", nil, "", fmt.Errorf("kubeconfig cluster %q not found", ctxEntry.Context.Cluster)
	}
	userEntry, _ := findUser(kc.Users, ctxEntry.Context.User)

	baseDir := filepath.Dir(cfg.KubeconfigPath)
	token := strings.TrimSpace(cfg.BearerToken)
	if token == "" {
		token = strings.TrimSpace(userEntry.User.Token)
	}

	caPath := resolvePath(firstNonEmpty(cfg.CAFile, clusterEntry.Cluster.CertificateAuthority), baseDir)
	clientCertPath := resolvePath(firstNonEmpty(cfg.ClientCertFile, userEntry.User.ClientCertificate), baseDir)
	clientKeyPath := resolvePath(firstNonEmpty(cfg.ClientKeyFile, userEntry.User.ClientKey), baseDir)

	tlsConfig, err := buildTLSConfig(tlsConfigInput{
		CAData:              firstNonEmpty(cfg.CAData, clusterEntry.Cluster.CertificateAuthorityData),
		CAFile:              caPath,
		InsecureSkipTLS:     cfg.InsecureSkipTLS || clusterEntry.Cluster.InsecureSkipTLSVerify,
		ClientCertFile:      clientCertPath,
		ClientKeyFile:       clientKeyPath,
		ClientCertData:      userEntry.User.ClientCertificateData,
		ClientKeyData:       userEntry.User.ClientKeyData,
		AssumeBase64ForData: true,
	})
	if err != nil {
		return "", "", nil, "", err
	}

	server := clusterEntry.Cluster.Server
	if server == "" {
		return "", "", nil, "", errors.New("kubeconfig cluster server is required")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}, Timeout: 15 * time.Second}

	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(ctxEntry.Context.Namespace)
	}
	return normalizeServerURL(server), token, client, namespace, nil
}

func findContext(entries []kubeContextEntry, name string) (kubeContextEntry, bool) {
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return kubeContextEntry{}, false
}

func findCluster(entries []kubeClusterEntry, name string) (kubeClusterEntry, bool) {
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return kubeClusterEntry{}, false
}

func findUser(entries []kubeUserEntry, name string) (kubeUserEntry, bool) {
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return kubeUserEntry{}, false
}

type tlsConfigInput struct {
	CAData              string
	CAFile              string
	InsecureSkipTLS     bool
	ClientCertFile      string
	ClientKeyFile       string
	ClientCertData      string
	ClientKeyData       string
	AssumeBase64ForData bool
}

func buildTLSConfig(input tlsConfigInput) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: input.InsecureSkipTLS} //nolint:gosec
	if input.CAData != "" || input.CAFile != "" {
		caPool := x509.NewCertPool()
		caBytes, err := loadPEMData(input.CAData, input.CAFile, input.AssumeBase64ForData)
		if err != nil {
			return nil, err
		}
		if len(caBytes) > 0 && !caPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("parse kubernetes ca cert")
		}
		cfg.RootCAs = caPool
	}

	if input.ClientCertData != "" || input.ClientCertFile != "" {
		certBytes, err := loadPEMData(input.ClientCertData, input.ClientCertFile, input.AssumeBase64ForData)
		if err != nil {
			return nil, err
		}
		keyBytes, err := loadPEMData(input.ClientKeyData, input.ClientKeyFile, input.AssumeBase64ForData)
		if err != nil {
			return nil, err
		}
		if len(certBytes) == 0 || len(keyBytes) == 0 {
			return nil, errors.New("client certificate and key are required together")
		}
		cert, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("parse client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func loadPEMData(dataValue, filePath string, base64Decode bool) ([]byte, error) {
	if dataValue != "" {
		return decodeMaybeBase64(dataValue, base64Decode)
	}
	if filePath == "" {
		return nil, nil
	}
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}
	return content, nil
}

func decodeMaybeBase64(value string, base64Decode bool) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if strings.Contains(value, "BEGIN CERTIFICATE") || strings.Contains(value, "BEGIN RSA PRIVATE KEY") || strings.Contains(value, "BEGIN PRIVATE KEY") {
		return []byte(value), nil
	}
	if base64Decode {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}
	return []byte(value), nil
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func resolvePath(path, baseDir string) string {
	trim := strings.TrimSpace(path)
	if trim == "" {
		return ""
	}
	if filepath.IsAbs(trim) || baseDir == "" {
		return trim
	}
	return filepath.Join(baseDir, trim)
}

func inClusterClient(_ context.Context) (string, string, *http.Client, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return "", "", nil, errors.New("kubernetes service host/port not found (is this running in cluster?)")
	}

	caPath := filepath.Join(serviceAccountPath, "ca.crt")
	caData, err := os.ReadFile(caPath)
	if err != nil {
		return "", "", nil, fmt.Errorf("read kubernetes ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return "", "", nil, errors.New("parse kubernetes ca cert")
	}

	tokenPath := filepath.Join(serviceAccountPath, "token")
	tokenData, err := os.ReadFile(tokenPath)
	if err != nil {
		return "", "", nil, fmt.Errorf("read kubernetes token: %w", err)
	}
	token := strings.TrimSpace(string(tokenData))

	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	baseURL := fmt.Sprintf("https://%s:%s", host, port)

	return baseURL, token, client, nil
}

func readNamespace() (string, error) {
	path := filepath.Join(serviceAccountPath, "namespace")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func buildJobName(prefix, flowID string) string {
	suffix := strings.ToLower(strings.TrimSpace(uuid.NewString()))
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	base := sanitizeName(prefix + "-" + flowID)
	maxBase := 63 - len(suffix) - 1
	if maxBase < 1 {
		maxBase = 1
	}
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	if base == "" {
		base = "flow"
	}
	return base + "-" + suffix
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
		out = append(out, fmt.Sprintf("-flow-id=%s", flowID))
	}
	if maxEmpty > 0 && !hasFlag(out, "max-empty-reads") {
		out = append(out, fmt.Sprintf("-max-empty-reads=%d", maxEmpty))
	}
	return out
}

func hasFlag(args []string, name string) bool {
	needle := "-" + name
	for _, arg := range args {
		if arg == needle || strings.HasPrefix(arg, needle+"=") || strings.HasPrefix(arg, "--"+name+"=") || arg == "--"+name {
			return true
		}
	}
	return false
}

func mapToEnvVars(values map[string]string) []envVar {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]envVar, 0, len(values))
	for _, key := range keys {
		out = append(out, envVar{Name: key, Value: values[key]})
	}
	return out
}

func parseEnvFrom(entries []string) []envFromSource {
	if len(entries) == 0 {
		return nil
	}
	out := make([]envFromSource, 0, len(entries))
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
			out = append(out, envFromSource{SecretRef: &nameRef{Name: name}})
		case "configmap", "configmapref", "config-map":
			out = append(out, envFromSource{ConfigMapRef: &nameRef{Name: name}})
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
	val := int32(value)
	return &val
}
