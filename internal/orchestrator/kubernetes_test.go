package orchestrator

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
)

func TestBuildJobName_SanitizesAndBounds(t *testing.T) {
	name := buildJobName("Wallaby$Worker", "Flow_ABC")
	if len(name) > 63 {
		t.Fatalf("expected name <= 63 chars, got %d", len(name))
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(name) {
		t.Fatalf("expected name sanitized, got %s", name)
	}
}

func TestKubernetesDispatcherCancelFlowForeground(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	flowID := "flow-1"
	prefix := "wallaby-worker"
	jobName := buildJobName(prefix, flowID)
	client := fake.NewClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}})
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{JobNamePrefix: prefix}}
	if err := dispatcher.CancelFlow(ctx, flowID); err != nil {
		t.Fatalf("CancelFlow() error = %v", err)
	}
	for _, action := range client.Actions() {
		deleteAction, ok := action.(interface{ GetDeleteOptions() metav1.DeleteOptions })
		if !ok || action.GetVerb() != "delete" {
			continue
		}
		options := deleteAction.GetDeleteOptions()
		if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
			t.Fatalf("propagation = %v, want foreground", options.PropagationPolicy)
		}
		return
	}
	t.Fatal("expected delete action")
}

func TestKubernetesDispatcherGenerationIdentityAndFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset()
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{
		JobImage: "wallaby:test", JobNamePrefix: "wallaby-worker", JobServiceAccount: "wallaby-worker",
		SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy", SnowflakeStreamingRESTConfigMapKey: "snowflake-streaming-rest-enabled",
	}}
	if err := dispatcher.EnqueueGeneration(ctx, "orders/eu", 7); err != nil {
		t.Fatal(err)
	}
	name := buildGenerationJobName("wallaby-worker", "orders/eu", 7)
	job, err := client.BatchV1().Jobs("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Annotations[generationMetadataKey] != "7" {
		t.Fatalf("generation annotation=%q", job.Annotations[generationMetadataKey])
	}
	if job.Labels[flowHashMetadataKey] != flowLabelValue("orders/eu") {
		t.Fatalf("flow hash label=%q", job.Labels[flowHashMetadataKey])
	}
	if _, exists := job.Labels[flowIDMetadataKey]; exists {
		t.Fatal("raw flow id must not be used as a Kubernetes label")
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	for _, value := range []string{"--flow-id", "orders/eu", "--generation", "7", "--execution-backend", kubernetesBackend, "--execution-id", name} {
		if !slices.Contains(args, value) {
			t.Fatalf("worker args=%v missing %q", args, value)
		}
	}
	if job.Spec.Template.Spec.AutomountServiceAccountToken == nil || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("automount=%v, want false", job.Spec.Template.Spec.AutomountServiceAccountToken)
	}
	var policyEnv *corev1.EnvVar
	for index := range job.Spec.Template.Spec.Containers[0].Env {
		candidate := &job.Spec.Template.Spec.Containers[0].Env[index]
		if candidate.Name == "WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED" {
			policyEnv = candidate
		}
	}
	if policyEnv == nil || policyEnv.ValueFrom == nil || policyEnv.ValueFrom.ConfigMapKeyRef == nil || policyEnv.ValueFrom.ConfigMapKeyRef.Name != "wallaby-runtime-policy" || policyEnv.ValueFrom.ConfigMapKeyRef.Key != "snowflake-streaming-rest-enabled" {
		t.Fatalf("non-Snowflake worker policy env=%+v", policyEnv)
	}
	if !slices.Contains(args, "--snowflake-streaming-rest-granted=false") {
		t.Fatalf("non-Snowflake worker did not carry explicit false dispatch grant: %v", args)
	}
}

func TestKubernetesDispatcherRequiresStablePolicyConfigMapForEveryJob(t *testing.T) {
	t.Parallel()
	for _, cfg := range []KubernetesConfig{
		{JobImage: "wallaby:test"},
		{JobImage: "wallaby:test", SnowflakeStreamingRESTConfigMapName: "policy"},
		{JobImage: "wallaby:test", SnowflakeStreamingRESTConfigMapKey: "enabled"},
	} {
		if _, err := NewKubernetesDispatcher(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "stable Streaming policy ConfigMap") {
			t.Fatalf("config=%+v error=%v", cfg, err)
		}
	}
}

func TestKubernetesDispatcherRunOnceUsesUniqueGenerationFencedJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset()
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{
		JobImage: "wallaby:test", JobNamePrefix: "wallaby-worker",
		SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy", SnowflakeStreamingRESTConfigMapKey: "snowflake-streaming-rest-enabled",
	}}
	for range 2 {
		if err := dispatcher.EnqueueRunOnce(ctx, "orders/eu", 7); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := client.BatchV1().Jobs("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 2 {
		t.Fatalf("run-once jobs=%d, want 2", len(jobs.Items))
	}
	if jobs.Items[0].Name == jobs.Items[1].Name {
		t.Fatalf("run-once attempts reused job %q", jobs.Items[0].Name)
	}
	for _, job := range jobs.Items {
		if job.Annotations[generationMetadataKey] != "7" {
			t.Fatalf("job %s generation=%q, want 7", job.Name, job.Annotations[generationMetadataKey])
		}
		args := job.Spec.Template.Spec.Containers[0].Args
		for _, value := range []string{"--generation", "7", "--execution-id", job.Name} {
			if !slices.Contains(args, value) {
				t.Fatalf("job %s args=%v missing %q", job.Name, args, value)
			}
		}
	}
}

func TestKubernetesDispatcherCancelThroughGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	flowID := "orders"
	oldName := buildGenerationJobName("wallaby-worker", flowID, 1)
	newName := buildGenerationJobName("wallaby-worker", flowID, 2)
	job := func(name, generation string) *batchv1.Job {
		return authoritativeJob(name, flowID, generation)
	}
	client := fake.NewClientset(job(oldName, "1"), job(newName, "2"))
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default"}
	receipt, err := dispatcher.CancelThroughGeneration(ctx, flowID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Terminal || receipt.ThroughGeneration != 1 {
		t.Fatalf("receipt=%+v", receipt)
	}
	wantTerminalIDs := []string{oldName, buildJobName("wallaby-worker", flowID)}
	slices.Sort(wantTerminalIDs)
	if !slices.Equal(receipt.TerminalExecutionIDs, wantTerminalIDs) {
		t.Fatalf("terminal execution ids=%v, want exact %v", receipt.TerminalExecutionIDs, wantTerminalIDs)
	}
	if _, err := client.BatchV1().Jobs("default").Get(ctx, oldName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old generation still exists: %v", err)
	}
	if _, err := client.BatchV1().Jobs("default").Get(ctx, newName, metav1.GetOptions{}); err != nil {
		t.Fatalf("new generation was deleted: %v", err)
	}
}

func TestKubernetesDispatcherReservedOwnershipCannotBeOverridden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset()
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{
		JobImage:      "wallaby:test",
		JobNamePrefix: "wallaby-worker",
		JobLabels: map[string]string{
			flowHashMetadataKey:    "attacker",
			generationMetadataKey:  "999",
			backendMetadataKey:     "worker",
			executionIDMetadataKey: "attacker",
		},
		JobAnnotations: map[string]string{
			flowIDMetadataKey: "attacker", generationMetadataKey: "999",
			backendMetadataKey: "worker", executionIDMetadataKey: "attacker",
		},
		SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy", SnowflakeStreamingRESTConfigMapKey: "snowflake-streaming-rest-enabled",
		JobEnv: map[string]string{
			"WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED": "true",
			"WALLABY_SNOWFLAKE_STREAMING_REST_ENABLED":        "true",
			"SAFE_ENV": "kept",
		},
		JobArgs: []string{
			"--", "--flow-id=attacker", "--generation", "999", "--execution-backend=worker", "--execution-id", "attacker",
			"--snowflake-enabled=true", "--snowflake-streaming-rest-granted=true", "--snowflake-account=attacker", "--snowflake-user=attacker", "--snowflake-host=attacker.example",
			"--snowflake-private-key-file=/attacker/key.pem", "--foo=bar",
		},
	}}
	if err := dispatcher.EnqueueGeneration(ctx, "orders", 3); err != nil {
		t.Fatal(err)
	}
	name := buildGenerationJobName("wallaby-worker", "orders", 3)
	job, err := client.BatchV1().Jobs("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Annotations[flowIDMetadataKey] != "orders" || job.Annotations[generationMetadataKey] != "3" ||
		job.Annotations[backendMetadataKey] != kubernetesBackend || job.Annotations[executionIDMetadataKey] != name {
		t.Fatalf("authoritative annotations were overridden: %v", job.Annotations)
	}
	if job.Labels[flowHashMetadataKey] != flowLabelValue("orders") || job.Labels[generationMetadataKey] != "3" ||
		job.Labels[backendMetadataKey] != kubernetesBackend || job.Labels[executionIDMetadataKey] != name {
		t.Fatalf("authoritative labels were overridden: %v", job.Labels)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	for _, forbidden := range []string{"--", "--flow-id=attacker", "999", "--execution-backend=worker", "attacker", "--snowflake-account=attacker", "--snowflake-user=attacker", "--snowflake-host=attacker.example"} {
		if slices.Contains(args, forbidden) {
			t.Fatalf("reserved job argument survived: %q in %v", forbidden, args)
		}
	}
	if !slices.Contains(args, "--foo=bar") {
		t.Fatalf("unrelated job arg was removed: %v", args)
	}
	if !slices.Contains(args, "--snowflake-enabled=false") || !slices.Contains(args, "--snowflake-streaming-rest-granted=false") || slices.Contains(args, "--snowflake-enabled=true") || slices.Contains(args, "--snowflake-streaming-rest-granted=true") || slices.Contains(args, "--snowflake-private-key-file=/attacker/key.pem") {
		t.Fatalf("deployment Snowflake policy was not authoritative: %v", args)
	}
	keyFlag := slices.Index(args, "--snowflake-private-key-file")
	if keyFlag < 0 || keyFlag+1 >= len(args) || args[keyFlag+1] != "" {
		t.Fatalf("disabled deployment did not pass an explicit empty key path: %v", args)
	}
	policyCount := 0
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		switch env.Name {
		case "WALLABY_SNOWFLAKE_STREAMING_REST_ENABLED":
			t.Fatalf("server policy alias survived in worker env: %+v", env)
		case "WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED":
			policyCount++
			if env.ValueFrom == nil || env.ValueFrom.ConfigMapKeyRef == nil {
				t.Fatalf("worker policy env was not sourced from ConfigMap: %+v", env)
			}
		case "SAFE_ENV":
			if env.Value != "kept" {
				t.Fatalf("safe env=%+v", env)
			}
		}
	}
	if policyCount != 1 {
		t.Fatalf("worker policy env count=%d, want one authoritative value", policyCount)
	}
}

func TestKubernetesExistingJobRejectsPolicyOrTemplateDigestDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset()
	base := KubernetesConfig{JobImage: "wallaby:v1", JobNamePrefix: "wallaby-worker", SnowflakePolicyFingerprint: strings.Repeat("a", 64), SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy", SnowflakeStreamingRESTConfigMapKey: "snowflake-streaming-rest-enabled"}
	first := &KubernetesDispatcher{client: client, namespace: "default", cfg: base}
	if err := first.EnqueueGeneration(ctx, "orders", 3); err != nil {
		t.Fatal(err)
	}
	if err := first.EnqueueGeneration(ctx, "orders", 3); err != nil {
		t.Fatalf("matching existing worker template was not adopted: %v", err)
	}
	for name, mutate := range map[string]func(*KubernetesConfig){
		"image":        func(cfg *KubernetesConfig) { cfg.JobImage = "wallaby:v2" },
		"key rotation": func(cfg *KubernetesConfig) { cfg.SnowflakePolicyFingerprint = strings.Repeat("b", 64) },
		"command":      func(cfg *KubernetesConfig) { cfg.JobCommand = []string{"/different-worker"} },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: cfg}
			err := dispatcher.EnqueueGeneration(ctx, "orders", 3)
			if err == nil || !strings.Contains(err.Error(), "stale Snowflake policy or pod template digest") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	jobName := buildGenerationJobName("wallaby-worker", "orders", 3)
	forged, err := client.BatchV1().Jobs("default").Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forged.Spec.Template.Spec.Containers[0].Image = "attacker/image:latest"
	// Preserve both forged digest annotations. Adoption must use the actual pod
	// projection rather than trusting either annotation.
	if _, err := client.BatchV1().Jobs("default").Update(ctx, forged, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := first.EnqueueGeneration(ctx, "orders", 3); err == nil || !strings.Contains(err.Error(), "non-authoritative worker pod template") {
		t.Fatalf("forged matching annotation error=%v", err)
	}
}

func TestKubernetesExistingJobCanonicalizesAPIDefaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := KubernetesConfig{
		JobImage: "wallaby:v1", JobNamePrefix: "wallaby-worker",
		SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy",
		SnowflakeStreamingRESTConfigMapKey:  "snowflake-streaming-rest-enabled",
	}
	seed := &KubernetesDispatcher{namespace: "default", cfg: cfg}
	job := seed.desiredJob("orders", buildGenerationJobName("wallaby-worker", "orders", 3), 3)
	kubernetesscheme.Scheme.Default(job)
	client := fake.NewClientset(job)
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: cfg}
	if err := dispatcher.EnqueueGeneration(ctx, "orders", 3); err != nil {
		t.Fatalf("API-defaulted existing template was not adopted: %v", err)
	}
}

func TestKubernetesExistingJobRejectsCompletePodTemplateDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := KubernetesConfig{
		JobImage: "wallaby:v1", JobNamePrefix: "wallaby-worker",
		SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy",
		SnowflakeStreamingRESTConfigMapKey:  "snowflake-streaming-rest-enabled",
	}
	for name, mutate := range map[string]func(*corev1.PodTemplateSpec){
		"forged annotation with init container": func(template *corev1.PodTemplateSpec) {
			template.Spec.InitContainers = append(template.Spec.InitContainers, corev1.Container{Name: "forged-init", Image: "attacker/init:latest"})
		},
		"privileged worker": func(template *corev1.PodTemplateSpec) {
			template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: boolPtr(true)}
		},
		"forged ephemeral container": func(template *corev1.PodTemplateSpec) {
			template.Spec.EphemeralContainers = append(template.Spec.EphemeralContainers, corev1.EphemeralContainer{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "forged-debug", Image: "attacker/debug:latest"}})
		},
		"altered envFrom": func(template *corev1.PodTemplateSpec) {
			template.Spec.Containers[0].EnvFrom = append(template.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "forged"}}})
		},
		"altered volume": func(template *corev1.PodTemplateSpec) {
			template.Spec.Volumes = append(template.Spec.Volumes, corev1.Volume{Name: "forged", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		},
		"altered service account": func(template *corev1.PodTemplateSpec) {
			template.Spec.ServiceAccountName = "forged-service-account"
		},
	} {
		t.Run(name, func(t *testing.T) {
			seed := &KubernetesDispatcher{namespace: "default", cfg: cfg}
			job := seed.desiredJob("orders", buildGenerationJobName("wallaby-worker", "orders", 3), 3)
			kubernetesscheme.Scheme.Default(job)
			mutate(&job.Spec.Template)
			client := fake.NewClientset(job)
			dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: cfg}
			if err := dispatcher.EnqueueGeneration(ctx, "orders", 3); err == nil || !strings.Contains(err.Error(), "non-authoritative worker pod template") {
				t.Fatalf("template drift error=%v", err)
			}
		})
	}
}

func TestKubernetesSnowflakeEnabledJobMountsAuthoritativeSecretKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset()
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{
		JobImage: "wallaby:test", JobNamePrefix: "wallaby-worker", SnowflakeEnabled: true,
		SnowflakeStreamingRESTEnabled: true, SnowflakeStreamingRESTConfigMapName: "wallaby-runtime-policy", SnowflakeStreamingRESTConfigMapKey: "snowflake-streaming-rest-enabled", SnowflakePolicyFingerprint: strings.Repeat("a", 64),
		SnowflakeAccount: "account", SnowflakeUser: "user", SnowflakeHost: "account.snowflakecomputing.com",
		SnowflakePrivateKeyFile:       "/run/secrets/wallaby/snowflake-key.pem",
		SnowflakePrivateKeySecretName: "wallaby-snowflake", SnowflakePrivateKeySecretKey: "private-key.pem",
	}}
	if err := dispatcher.EnqueueGeneration(ctx, "orders", 3); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("default").Get(ctx, buildGenerationJobName("wallaby-worker", "orders", 3), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	if len(pod.Volumes) != 1 || pod.Volumes[0].Secret == nil || pod.Volumes[0].Secret.SecretName != "wallaby-snowflake" || len(pod.Volumes[0].Secret.Items) != 1 || pod.Volumes[0].Secret.Items[0].Key != "private-key.pem" || pod.Volumes[0].Secret.Items[0].Mode == nil || *pod.Volumes[0].Secret.Items[0].Mode != 0o440 {
		t.Fatalf("Snowflake key volume=%+v", pod.Volumes)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 65532 || pod.SecurityContext.FSGroupChangePolicy == nil || *pod.SecurityContext.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		t.Fatalf("Snowflake pod security context=%+v", pod.SecurityContext)
	}
	if job.Annotations["wallaby.snowflake-policy-digest"] == "" || job.Annotations["wallaby.snowflake-policy-digest"] != job.Spec.Template.Annotations["wallaby.snowflake-policy-digest"] {
		t.Fatalf("Snowflake policy digest annotations=%v/%v", job.Annotations, job.Spec.Template.Annotations)
	}
	container := pod.Containers[0]
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].MountPath != "/run/secrets/wallaby/snowflake-key.pem" || container.VolumeMounts[0].SubPath != "private-key.pem" || !container.VolumeMounts[0].ReadOnly {
		t.Fatalf("Snowflake key mount=%+v", container.VolumeMounts)
	}
	for _, want := range []string{"--snowflake-enabled=true", "--snowflake-streaming-rest-granted=true", "--snowflake-account", "account", "--snowflake-user", "user", "--snowflake-host", "account.snowflakecomputing.com", "--snowflake-private-key-file", "/run/secrets/wallaby/snowflake-key.pem", "--snowflake-policy-digest", strings.Repeat("a", 64)} {
		if !slices.Contains(container.Args, want) {
			t.Fatalf("missing authoritative argument %q in %v", want, container.Args)
		}
	}
	var policyEnv *corev1.EnvVar
	for index := range container.Env {
		if container.Env[index].Name == "WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED" {
			policyEnv = &container.Env[index]
		}
	}
	if policyEnv == nil || policyEnv.ValueFrom == nil || policyEnv.ValueFrom.ConfigMapKeyRef == nil || policyEnv.ValueFrom.ConfigMapKeyRef.Name != "wallaby-runtime-policy" || policyEnv.ValueFrom.ConfigMapKeyRef.Key != "snowflake-streaming-rest-enabled" {
		t.Fatalf("Snowpipe Streaming policy env=%+v", policyEnv)
	}
}

func TestKubernetesCancellationRejectsMalformedOwnershipMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	flowID := "orders"
	name := buildGenerationJobName("wallaby-worker", flowID, 1)
	malformed := authoritativeJob(name, flowID, "not-a-generation")
	client := fake.NewClientset(malformed)
	dispatcher := &KubernetesDispatcher{client: client, namespace: "default", cfg: KubernetesConfig{JobNamePrefix: "wallaby-worker"}}
	receipt, err := dispatcher.CancelThroughGeneration(ctx, flowID, 1)
	if err == nil || receipt.Terminal {
		t.Fatalf("CancelThroughGeneration()=(%+v,%v), want fail-closed malformed metadata", receipt, err)
	}
	if _, getErr := client.BatchV1().Jobs("default").Get(ctx, name, metav1.GetOptions{}); getErr != nil {
		t.Fatalf("malformed job was deleted: %v", getErr)
	}
}

func authoritativeJob(name, flowID, generation string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "default",
		Annotations: map[string]string{
			flowIDMetadataKey: flowID, generationMetadataKey: generation,
			backendMetadataKey: kubernetesBackend, executionIDMetadataKey: name,
		},
		Labels: map[string]string{
			flowHashMetadataKey: flowLabelValue(flowID), generationMetadataKey: generation,
			backendMetadataKey: kubernetesBackend, executionIDMetadataKey: name,
		},
	}}
}

func TestParseEnvFrom(t *testing.T) {
	entries := []string{"secret:foo", "configmap:bar", "bad"}
	out := parseEnvFrom(entries)
	if len(out) != 2 {
		t.Fatalf("expected 2 envFrom entries, got %d", len(out))
	}
	if out[0].SecretRef == nil || out[0].SecretRef.Name != "foo" {
		t.Fatalf("expected secret ref foo, got %#v", out[0].SecretRef)
	}
	if out[1].ConfigMapRef == nil || out[1].ConfigMapRef.Name != "bar" {
		t.Fatalf("expected configmap ref bar, got %#v", out[1].ConfigMapRef)
	}
}
