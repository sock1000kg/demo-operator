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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cmpv1alpha1 "github.com/sock1000kg/demo-operator/api/v1alpha1"
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
	log := logf.FromContext(ctx)

	// Fetch the Workspace instance
	var workspace cmpv1alpha1.Workspace
	if err := r.Get(ctx, req.NamespacedName, &workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Check if the object is under deletion
	isMarkedToBeDeleted := workspace.GetDeletionTimestamp() != nil
	if isMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(&workspace, workspaceFinalizer) {
			log.Info("Deleting external k3d cluster", "workspace", workspace.Name)

			// EXECUTE K3D TEARDOWN
			err := r.Provisioner.DeleteCluster(ctx, workspace.Name)
			if err != nil {
				return ctrl.Result{}, err
			}

			// Once cleanup is successful, remove the finalizer from the list
			controllerutil.RemoveFinalizer(&workspace, workspaceFinalizer)

			// Update the object to strip the finalizer.
			// Kubernetes will now automatically delete the object from etcd.
			if err := r.Update(ctx, &workspace); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation because the item is being deleted
		return ctrl.Result{}, nil
	}

	// The object is NOT being deleted.
	// Ensure our finalizer is attached before we provision anything.
	if !controllerutil.ContainsFinalizer(&workspace, workspaceFinalizer) {
		controllerutil.AddFinalizer(&workspace, workspaceFinalizer)
		if err := r.Update(ctx, &workspace); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check if cluster exists
	exists, err := r.Provisioner.Exists(ctx, workspace.Name)
	if err != nil {
		log.Error(err, "Failed to check if cluster exists")
		return ctrl.Result{}, err
	}

	if !exists {
		log.Info("Provisioning new cluster", "Workspace", workspace.Name)
		kubeconfig, err := r.Provisioner.CreateCluster(ctx, workspace.Name, workspace.Spec.NodeCount)
		if err != nil {
			log.Error(err, "Failed to provision cluster")
			return ctrl.Result{}, err
		}

		// Store Kubeconfig in a Secret in the Management Cluster
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      workspace.Name + "-kubeconfig",
				Namespace: workspace.Namespace,
			},
			StringData: map[string]string{
				"kubeconfig": kubeconfig,
			},
		}

		// CreateOrUpdate fetches the existing Secret (if any) and passes it to the mutate function.
		op, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
			// 1. Set the OwnerReference for automatic cleanup.
			if err := controllerutil.SetControllerReference(&workspace, secret, r.Scheme); err != nil {
				return err
			}

			// 2. Define or update the desired state of the Secret.
			// If the Secret already exists, this overwrites the old, stale kubeconfig.
			if secret.Data == nil {
				log.Info("Kubeconfig Secret has no data", "secret", secret.Name)
				secret.Data = make(map[string][]byte)
			} else if _, exists := secret.Data["kubeconfig"]; exists {
				log.Info("Kubeconfig already exists, updating it", "secret", secret.Name)
			} else {
				log.Info("Kubeconfig Secret exists but has no kubeconfig", "secret", secret.Name)
			}
			secret.Data["kubeconfig"] = []byte(kubeconfig)
			secret.Type = corev1.SecretTypeOpaque

			return nil
		})

		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to reconcile kubeconfig secret: %w", err)
		}

		// Log the operation (created, updated, or unchanged) for debugging
		log.Info("Reconciled kubeconfig secret", "operation", op)

		// Update Workspace status
		workspace.Status.Ready = true
		workspace.Status.KubeConfig = secret.Name
		if err := r.Status().Update(ctx, &workspace); err != nil {
			log.Error(err, "Failed to update Workspace status")
			return ctrl.Result{}, err
		}

		log.Info("Successfully provisioned cluster and updated status", "Workspace", workspace.Name)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cmpv1alpha1.Workspace{}).
		Named("workspace").
		Complete(r)
}
