package controller

import (
	"context"
	"fmt"

	cmpv1alpha1 "github.com/sock1000kg/demo-operator/api/v1alpha1"
)

func (r *WorkspaceReconciler) updateStatus(
	ctx context.Context,
	workspace *cmpv1alpha1.Workspace,
	phase string,
	ready bool,
) error {
	workspace.Status.Phase = phase
	workspace.Status.Ready = ready

	if err := r.Status().Update(ctx, workspace); err != nil {
		return fmt.Errorf("failed to update Workspace status: %w", err)
	}

	return nil
}
