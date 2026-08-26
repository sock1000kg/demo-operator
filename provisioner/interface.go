package provisioner

import "context"

// ClusterProvisioner abstracts the infrastructure layer.
type ClusterProvisioner interface {
	// CreateCluster provisions a new Kubernetes cluster and returns its kubeconfig as a string.
	CreateCluster(ctx context.Context, name string, nodeCount int) (string, error)
	// DeleteCluster removes the infrastructure.
	DeleteCluster(ctx context.Context, name string) error
	// Exists checks if the cluster is already running.
	Exists(ctx context.Context, name string) (bool, error)
}