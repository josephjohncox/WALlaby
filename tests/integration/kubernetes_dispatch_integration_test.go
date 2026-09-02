package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/orchestrator"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func TestKubernetesDispatcherIntegration(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	inCluster := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
	if kubeconfig == "" && !inCluster {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set and not running in-cluster")
	}

	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	image := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_IMAGE"))
	if image == "" {
		image = "busybox:1.36"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := kubeClient(kubeconfig)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}
	policyConfigMap := createStreamingPolicyConfigMap(t, ctx, client, namespace, "wallaby-test-policy")
	dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
		KubeconfigPath:                      kubeconfig,
		Namespace:                           namespace,
		JobImage:                            image,
		JobCommand:                          []string{"/bin/true"},
		JobNamePrefix:                       "wallaby-test",
		MaxEmptyReads:                       1,
		SnowflakeStreamingRESTConfigMapName: policyConfigMap,
		SnowflakeStreamingRESTConfigMapKey:  testStreamingPolicyConfigMapKey,
	})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}

	flowID := fmt.Sprintf("flow-%d", time.Now().UnixNano())
	if err := dispatcher.EnqueueRunOnce(ctx, flowID, 1); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
		if err != nil {
			return false, err
		}
		return len(jobs) > 0, nil
	})

	jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, job := range jobs {
		assertJobPolicyConfigMap(t, job, policyConfigMap)
		_ = deleteJob(ctx, client, namespace, job.Name)
	}
}

func TestKubernetesDispatcherJobSpec(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	inCluster := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
	if kubeconfig == "" && !inCluster {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set and not running in-cluster")
	}

	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	image := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_IMAGE"))
	if image == "" {
		image = "busybox:1.36"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := kubeClient(kubeconfig)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cmName := "wallaby-env-" + suffix
	secretName := "wallaby-secret-" + suffix
	_, err = client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName},
		Data: map[string]string{
			"FOO":                           "BAR",
			testStreamingPolicyConfigMapKey: "false",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create configmap: %v", err)
	}
	defer func() {
		_ = client.CoreV1().ConfigMaps(namespace).Delete(context.Background(), cmName, metav1.DeleteOptions{})
	}()

	_, err = client.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName},
		StringData: map[string]string{"TOKEN": "secret"},
		Type:       corev1.SecretTypeOpaque,
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create secret: %v", err)
	}
	defer func() {
		_ = client.CoreV1().Secrets(namespace).Delete(context.Background(), secretName, metav1.DeleteOptions{})
	}()

	dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
		KubeconfigPath:     kubeconfig,
		Namespace:          namespace,
		JobImage:           image,
		JobImagePullPolicy: "IfNotPresent",
		JobServiceAccount:  "default",
		JobNamePrefix:      "wallaby-spec",
		JobTTLSeconds:      30,
		JobBackoffLimit:    1,
		MaxEmptyReads:      3,
		JobLabels: map[string]string{
			"wallaby.test-label": "true",
		},
		JobAnnotations: map[string]string{
			"wallaby.test-annotation": "ok",
		},
		JobCommand: []string{"/bin/true"},
		JobArgs:    []string{"--log-level=debug"},
		JobEnv: map[string]string{
			"WALLABY_ENV": "test",
		},
		JobEnvFrom: []string{
			"configmap:" + cmName,
			"secret:" + secretName,
		},
		SnowflakeStreamingRESTConfigMapName: cmName,
		SnowflakeStreamingRESTConfigMapKey:  testStreamingPolicyConfigMapKey,
	})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}

	flowID := fmt.Sprintf("flow-%d", time.Now().UnixNano())
	if err := dispatcher.EnqueueRunOnce(ctx, flowID, 1); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	var jobName string
	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
		if err != nil {
			return false, err
		}
		if len(jobs) == 0 {
			return false, nil
		}
		jobName = jobs[0].Name
		return true, nil
	})

	job, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	defer func() {
		_ = deleteJob(context.Background(), client, namespace, jobName)
	}()

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 1 {
		t.Fatalf("expected backoff limit 1, got %+v", job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 30 {
		t.Fatalf("expected ttl 30, got %+v", job.Spec.TTLSecondsAfterFinished)
	}
	if job.Annotations["wallaby.flow-id"] != flowID {
		t.Fatalf("expected authoritative flow annotation %s, got %s", flowID, job.Annotations["wallaby.flow-id"])
	}
	if job.Labels["wallaby.test-label"] != "true" {
		t.Fatalf("expected custom label, got %v", job.Labels)
	}
	if job.Annotations["wallaby.test-annotation"] != "ok" {
		t.Fatalf("expected custom annotation, got %v", job.Annotations)
	}

	if len(job.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected worker container")
	}
	ctr := job.Spec.Template.Spec.Containers[0]
	if ctr.Image != image {
		t.Fatalf("expected image %s, got %s", image, ctr.Image)
	}
	if ctr.ImagePullPolicy != corev1.PullPolicy("IfNotPresent") {
		t.Fatalf("expected pull policy IfNotPresent, got %s", ctr.ImagePullPolicy)
	}
	if !argValuePresent(ctr.Args, "flow-id", flowID) {
		t.Fatalf("expected authoritative --flow-id arg, got %v", ctr.Args)
	}
	if !argValuePresent(ctr.Args, "generation", "1") {
		t.Fatalf("expected generation fence arg, got %v", ctr.Args)
	}
	if !argValuePresent(ctr.Args, "max-empty-reads", "3") {
		t.Fatalf("expected --max-empty-reads arg, got %v", ctr.Args)
	}
	if !envPresent(ctr.Env, "WALLABY_ENV", "test") {
		t.Fatalf("expected env WALLABY_ENV=test, got %v", ctr.Env)
	}
	if !envFromPresent(ctr.EnvFrom, cmName, secretName) {
		t.Fatalf("expected envFrom configmap %s and secret %s, got %v", cmName, secretName, ctr.EnvFrom)
	}
	if !envConfigMapRefPresent(ctr.Env, "WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED", cmName, testStreamingPolicyConfigMapKey) {
		t.Fatalf("expected exact Streaming policy ConfigMap reference %s/%s, got %v", cmName, testStreamingPolicyConfigMapKey, ctr.Env)
	}
	assertJobPolicyConfigMap(t, *job, cmName)
	if job.Spec.Template.Spec.ServiceAccountName != "default" {
		t.Fatalf("expected service account default, got %s", job.Spec.Template.Spec.ServiceAccountName)
	}
}

func TestKubernetesDispatcherRetry(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	inCluster := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
	if kubeconfig == "" && !inCluster {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set and not running in-cluster")
	}

	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	image := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_IMAGE"))
	if image == "" {
		image = "busybox:1.36"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := kubeClient(kubeconfig)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}
	policyConfigMap := createStreamingPolicyConfigMap(t, ctx, client, namespace, "wallaby-retry-policy")
	dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
		KubeconfigPath:                      kubeconfig,
		Namespace:                           namespace,
		JobImage:                            image,
		JobNamePrefix:                       "wallaby-retry",
		JobBackoffLimit:                     1,
		JobCommand:                          []string{"/bin/sh", "-c"},
		JobArgs:                             []string{"exit 1"},
		SnowflakeStreamingRESTConfigMapName: policyConfigMap,
		SnowflakeStreamingRESTConfigMapKey:  testStreamingPolicyConfigMapKey,
	})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}

	flowID := fmt.Sprintf("flow-%d", time.Now().UnixNano())
	if err := dispatcher.EnqueueRunOnce(ctx, flowID, 1); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	var jobName string
	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
		if err != nil {
			return false, err
		}
		if len(jobs) == 0 {
			return false, nil
		}
		jobName = jobs[0].Name
		return true, nil
	})

	waitFor(t, 60*time.Second, 2*time.Second, func() (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 1 {
			t.Fatalf("expected backoff limit 1, got %+v", job.Spec.BackoffLimit)
		}
		assertJobPolicyConfigMap(t, *job, policyConfigMap)
		if job.Status.Failed > 0 {
			return true, nil
		}
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})

	_ = deleteJob(ctx, client, namespace, jobName)
}

func TestKubernetesDispatcherRecovery(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	inCluster := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
	if kubeconfig == "" && !inCluster {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set and not running in-cluster")
	}

	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	image := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_IMAGE"))
	if image == "" {
		image = "busybox:1.36"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client, err := kubeClient(kubeconfig)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}
	policyConfigMap := createStreamingPolicyConfigMap(t, ctx, client, namespace, "wallaby-recover-policy")
	dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
		KubeconfigPath:                      kubeconfig,
		Namespace:                           namespace,
		JobImage:                            image,
		JobNamePrefix:                       "wallaby-recover",
		JobBackoffLimit:                     0,
		JobCommand:                          []string{"/bin/true"},
		SnowflakeStreamingRESTConfigMapName: policyConfigMap,
		SnowflakeStreamingRESTConfigMapKey:  testStreamingPolicyConfigMapKey,
	})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}

	flowID := fmt.Sprintf("flow-%d", time.Now().UnixNano())
	if err := dispatcher.EnqueueRunOnce(ctx, flowID, 1); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	var firstJob string
	var firstUID string
	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
		if err != nil {
			return false, err
		}
		if len(jobs) == 0 {
			return false, nil
		}
		firstJob = jobs[0].Name
		firstUID = string(jobs[0].UID)
		assertJobPolicyConfigMap(t, jobs[0], policyConfigMap)
		return true, nil
	})

	waitFor(t, 60*time.Second, 2*time.Second, func() (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, firstJob, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})

	if err := dispatcher.EnqueueRunOnce(ctx, flowID, 1); err != nil {
		t.Fatalf("enqueue flow after completion: %v", err)
	}

	var secondJob string
	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
		if err != nil {
			return false, err
		}
		for _, job := range jobs {
			if job.Name != firstJob {
				secondJob = job.Name
				assertJobPolicyConfigMap(t, job, policyConfigMap)
				if string(job.UID) == firstUID {
					t.Fatalf("separate run-once attempts reused UID %s", firstUID)
				}
				return true, nil
			}
		}
		return false, nil
	})
	if secondJob == "" {
		t.Fatal("second run-once job was not created")
	}

	jobs, err := listJobsForFlow(ctx, client, namespace, flowID)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, job := range jobs {
		_ = deleteJob(ctx, client, namespace, job.Name)
	}
}

const testStreamingPolicyConfigMapKey = "snowflake-streaming-rest-enabled"

func createStreamingPolicyConfigMap(t *testing.T, ctx context.Context, client *kubernetes.Clientset, namespace, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if _, err := client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data:       map[string]string{testStreamingPolicyConfigMapKey: "false"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Streaming policy ConfigMap: %v", err)
	}
	t.Cleanup(func() {
		_ = client.CoreV1().ConfigMaps(namespace).Delete(context.Background(), name, metav1.DeleteOptions{})
	})
	return name
}

func kubeClient(kubeconfig string) (*kubernetes.Clientset, error) {
	if strings.TrimSpace(kubeconfig) != "" {
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
		clientCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
		restCfg, err := clientCfg.ClientConfig()
		if err != nil {
			return nil, err
		}
		return kubernetes.NewForConfig(restCfg)
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(restCfg)
}

func deleteJob(ctx context.Context, client *kubernetes.Clientset, namespace, name string) error {
	policy := metav1.DeletePropagationBackground
	return client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func listJobsForFlow(ctx context.Context, client kubernetes.Interface, namespace, flowID string) ([]batchv1.Job, error) {
	list, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	jobs := make([]batchv1.Job, 0, len(list.Items))
	for _, job := range list.Items {
		if job.Annotations["wallaby.flow-id"] == flowID {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func argValuePresent(args []string, name, value string) bool {
	flag := "--" + name
	for index, arg := range args {
		if arg == flag && index+1 < len(args) && args[index+1] == value {
			return true
		}
		if arg == flag+"="+value {
			return true
		}
	}
	return false
}

func assertJobPolicyConfigMap(t *testing.T, job batchv1.Job, configMap string) {
	t.Helper()
	if len(job.Spec.Template.Spec.Containers) == 0 || !envConfigMapRefPresent(
		job.Spec.Template.Spec.Containers[0].Env,
		"WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED",
		configMap,
		testStreamingPolicyConfigMapKey,
	) {
		t.Fatalf("job %s lacks exact Streaming policy ConfigMap reference %s/%s", job.Name, configMap, testStreamingPolicyConfigMapKey)
	}
}

func envConfigMapRefPresent(env []corev1.EnvVar, name, configMap, key string) bool {
	for _, item := range env {
		if item.Name == name && item.ValueFrom != nil && item.ValueFrom.ConfigMapKeyRef != nil &&
			item.ValueFrom.ConfigMapKeyRef.Name == configMap && item.ValueFrom.ConfigMapKeyRef.Key == key {
			return true
		}
	}
	return false
}

func envPresent(env []corev1.EnvVar, key, value string) bool {
	for _, item := range env {
		if item.Name == key && item.Value == value {
			return true
		}
	}
	return false
}

func envFromPresent(envFrom []corev1.EnvFromSource, configMap, secret string) bool {
	var hasConfigMap bool
	var hasSecret bool
	for _, item := range envFrom {
		if item.ConfigMapRef != nil && item.ConfigMapRef.Name == configMap {
			hasConfigMap = true
		}
		if item.SecretRef != nil && item.SecretRef.Name == secret {
			hasSecret = true
		}
	}
	return hasConfigMap && hasSecret
}
