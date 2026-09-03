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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cmpv1alpha1 "github.com/sock1000kg/demo-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sock1000kg/demo-operator/provisioner"
)

const workspaceFinalizer = "workspace.cmp.example.com/finalizer"

// WorkspaceReconciler reconciles a Workspace object
type WorkspaceReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Provisioner provisioner.ClusterProvisioner
}

// +kubebuilder:rbac:groups=cmp.example.com,resources=workspaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cmp.example.com,resources=workspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cmp.example.com,resources=workspaces/finalizers,verbs=update
//
// Additional permissions
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Workspace object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *WorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the Workspace instance
	var workspace cmpv1alpha1.Workspace
	if err := r.Get(ctx, req.NamespacedName, &workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Handle Deletion and Finalizers
	if !workspace.DeletionTimestamp.IsZero() {
		if err := r.updateStatus(ctx, &workspace, "Deleting", false); err != nil {
			return ctrl.Result{}, err
		}

		return r.handleDeletion(ctx, &workspace)
	}

	// 3. Ensure finalizer
	if !controllerutil.ContainsFinalizer(&workspace, workspaceFinalizer) {
		controllerutil.AddFinalizer(&workspace, workspaceFinalizer)

		if err := r.Update(ctx, &workspace); err != nil {
			return ctrl.Result{}, err
		}

		// Reconcile again with the updated object.
		return ctrl.Result{}, nil
	}

	// 3. Provision the Cluster via the abstract interface
	// Check if cluster exists
	exists, err := r.Provisioner.Exists(ctx, workspace.Name)
	if err != nil {
		logger.Error(err, "Failed to check if cluster exists")
		return ctrl.Result{}, err
	}

	if !exists {
		kubeconfigBytes, err := r.Provisioner.CreateCluster(ctx, workspace.Name, workspace.Spec.NodeCount)
		if err != nil {
			logger.Error(err, "Failed to provision cluster")
			if err := r.updateStatus(ctx, &workspace, "Deleting", false); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}

		// 4. Reconcile Child Resources (Secrets, etc.)
		if err := r.reconcileKubeconfigSecret(ctx, &workspace, []byte(kubeconfigBytes)); err != nil {
			logger.Error(err, "Failed to reconcile Kubeconfig Secret")
			// Replaced manual status update with helper
			_ = r.updateStatus(ctx, &workspace, "Failed", false)
			return ctrl.Result{}, err
		}
	}

	// 5. Update Status to Ready
	if err := r.updateStatus(ctx, &workspace, "Ready", true); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Successfully reconciled Workspace")
	return ctrl.Result{}, nil
}

func (r *WorkspaceReconciler) handleDeletion(ctx context.Context, workspace *cmpv1alpha1.Workspace) (ctrl.Result, error) {
	// Nothing to clean up if our finalizer isn't present.
	if !controllerutil.ContainsFinalizer(workspace, workspaceFinalizer) {
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)

	logger.Info(
		"Deleting external k3d cluster",
		"workspace", workspace.Name,
	)

	// Delete the external cluster.
	if err := r.Provisioner.DeleteCluster(ctx, workspace.Name); err != nil {
		logger.Error(err, "Failed to delete external k3d cluster")
		return ctrl.Result{}, err
	}

	// Cleanup succeeded, so remove the finalizer.
	controllerutil.RemoveFinalizer(workspace, workspaceFinalizer)

	// Kubernetes can now delete the Workspace from etcd.
	if err := r.Update(ctx, workspace); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileKubeconfigSecret ensures the secret containing the cluster credentials exists and is up-to-date.
func (r *WorkspaceReconciler) reconcileKubeconfigSecret(ctx context.Context, workspace *cmpv1alpha1.Workspace, kubeconfigData []byte) error {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-kubeconfig", workspace.Name),
			Namespace: workspace.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		// Set the Workspace as the owner so Kubernetes handles garbage collection.
		if err := controllerutil.SetControllerReference(workspace, secret, r.Scheme); err != nil {
			return err
		}

		if secret.Data == nil {
			secret.Data = make(map[string][]byte)
		}

		secret.Data["kubeconfig"] = kubeconfigData
		secret.Type = corev1.SecretTypeOpaque

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to CreateOrUpdate secret: %w", err)
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("Reconciled Secret", "operation", op, "name", secret.Name)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cmpv1alpha1.Workspace{}).
		Named("workspace").
		Complete(r)
}
