package k3d

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

type K3dProvisioner struct{}

func New() *K3dProvisioner {
	return &K3dProvisioner{}
}

func (p *K3dProvisioner) CreateCluster(ctx context.Context, name string, nodeCount int) (string, error) {
	// 1. Create the cluster using k3d
	cmd := exec.CommandContext(ctx, "k3d", "cluster", "create", name, fmt.Sprintf("--agents=%d", nodeCount))
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to create k3d cluster: %w", err)
	}

	// 2. Retrieve the kubeconfig
	kubeconfigCmd := exec.CommandContext(ctx, "k3d", "kubeconfig", "get", name)
	var out bytes.Buffer
	kubeconfigCmd.Stdout = &out
	if err := kubeconfigCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	return out.String(), nil
}

func (p *K3dProvisioner) DeleteCluster(ctx context.Context, name string) error {
	cmd := exec.CommandContext(ctx, "k3d", "cluster", "delete", name)
	return cmd.Run()
}

func (p *K3dProvisioner) Exists(ctx context.Context, name string) (bool, error) {
	cmd := exec.CommandContext(ctx, "k3d", "cluster", "get", name)
	err := cmd.Run()
	return err == nil, nil
}
