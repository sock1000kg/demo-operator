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
	fmt.Printf("Creating k3d cluster %q...\n", name)

	// 1. Create the cluster using k3d
	cmd := exec.CommandContext(ctx, "k3d", "cluster", "create", name,
		fmt.Sprintf("--agents=%d", nodeCount),
		"--kubeconfig-update-default=false",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		// This will now print the actual k3d error, e.g., "cluster already exists" or "docker daemon not running"
		return "", fmt.Errorf("failed to create k3d cluster: %w, output: %s", err, string(output))
	}

	fmt.Printf("k3d cluster %q created. Getting kubeconfig...\n", name)

	// 2. Retrieve the kubeconfig
	kubeconfigCmd := exec.CommandContext(ctx, "k3d", "kubeconfig", "get", name)
	var out bytes.Buffer
	kubeconfigCmd.Stdout = &out
	if err := kubeconfigCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	fmt.Printf("Got kubeconfig for %q.\n", name)

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
