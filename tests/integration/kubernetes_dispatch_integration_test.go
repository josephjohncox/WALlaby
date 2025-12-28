package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/orchestrator"
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

	dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
		KubeconfigPath: kubeconfig,
		Namespace:      namespace,
		JobImage:       image,
		JobCommand:     []string{"/bin/true"},
		JobNamePrefix:  "wallaby-test",
		MaxEmptyReads:  1,
	})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}

	flowID := fmt.Sprintf("flow-%d", time.Now().UnixNano())
	if err := dispatcher.EnqueueFlow(ctx, flowID); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	client, err := kubeClient(kubeconfig)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	waitFor(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		list, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("wallaby.flow-id=%s", flowID),
		})
		if err != nil {
			return false, err
		}
		return len(list.Items) > 0, nil
	})

	jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("wallaby.flow-id=%s", flowID),
	})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, job := range jobs.Items {
		_ = deleteJob(ctx, client, namespace, job.Name)
	}
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
