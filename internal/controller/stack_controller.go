/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cmpv1alpha1 "github.com/sock1000kg/demo-operator/api/v1alpha1"
)

// StackReconciler reconciles a Stack object
type StackReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cmp.example.com,resources=stacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cmp.example.com,resources=stacks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cmp.example.com,resources=stacks/finalizers,verbs=update
//
// Additional permissions
// +kubebuilder:rbac:groups=cmp.example.com,resources=workspaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Stack object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *StackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch Stack
	var stack cmpv1alpha1.Stack
	if err := r.Get(ctx, req.NamespacedName, &stack); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Fetch the target Workspace
	var workspace cmpv1alpha1.Workspace
	workspaceName := types.NamespacedName{Name: stack.Spec.WorkspaceRef, Namespace: req.Namespace}
	if err := r.Get(ctx, workspaceName, &workspace); err != nil {
		// Requeue if workspace isn't found or isn't ready yet
		log.Info("Workspace not found, requeuing", "workspace", stack.Spec.WorkspaceRef)
		return ctrl.Result{Requeue: true}, nil
	}

	if !workspace.Status.Ready {
		log.Info("Workspace is not ready yet, requeuing", "workspace", stack.Spec.WorkspaceRef)
		return ctrl.Result{Requeue: true}, nil
	}

	// 3. Fetch the Kubeconfig Secret from the Management Cluster
	var secret corev1.Secret
	secretName := types.NamespacedName{Name: workspace.Status.KubeConfig, Namespace: req.Namespace}
	if err := r.Get(ctx, secretName, &secret); err != nil {
		log.Error(err, "Failed to fetch kubeconfig secret", "secret", secretName)
		return ctrl.Result{}, err
	}

	kubeconfig := secret.Data["value"]

	// 4. Connect to Workspace and Deploy
	// targetClient, _ := createClientFromKubeconfig(kubeconfig)
	// deployer.Deploy(ctx, targetClient, stack.Spec.AppType)

	_ = kubeconfig // Suppress unused variable error

	log.Info("Successfully reconciled Stack")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cmpv1alpha1.Stack{}).
		Named("stack").
		Complete(r)
}
