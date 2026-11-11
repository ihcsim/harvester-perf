package k8s

import (
	"context"
	"fmt"
	"time"

	"github.com/harvester/hvperf/pkg/suites"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	watchapi "k8s.io/apimachinery/pkg/watch"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"
)

// EnsureNamespace ensures that the specified namespace exists. If the namespace
// does not exist, it will be created.
func EnsureNamespace(ctx context.Context, clients *suites.Clients, namespace string, readyTimeout time.Duration) (*corev1.Namespace, error) {
	ns, err := clients.K8sClientSet.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if !apierrs.IsNotFound(err) {
			return nil, err
		}
	}
	if !ns.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("namespace %s is being deleted - can't continue", namespace)
	}

	applyConfig := corev1apply.Namespace(namespace)
	created, err := clients.K8sClientSet.CoreV1().Namespaces().Apply(ctx, applyConfig, metav1.ApplyOptions{
		FieldManager: DefaultSSAFieldManager,
	})
	if err != nil {
		return nil, err
	}

	// wait till namespace is ready
	listWatch := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fmt.Sprintf("metadata.name=%s", created.GetName())
			return clients.K8sClientSet.CoreV1().Namespaces().List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watchapi.Interface, error) {
			options.FieldSelector = fmt.Sprintf("metadata.name=%s", created.GetName())
			return clients.K8sClientSet.CoreV1().Namespaces().Watch(ctx, options)
		},
	}
	ctxWithTimeout, cancel := watch.ContextWithOptionalTimeout(ctx, readyTimeout)
	defer cancel()
	if _, err := watch.UntilWithSync(ctxWithTimeout, listWatch, &corev1.Namespace{}, nil, func(event watchapi.Event) (bool, error) {
		ns, ok := event.Object.(*corev1.Namespace)
		if !ok {
			return false, fmt.Errorf("unexpected object type: %T", event.Object)
		}

		return ns.Status.Phase == "Active", nil
	}); err != nil {
		return nil, err
	}

	return created, nil
}
