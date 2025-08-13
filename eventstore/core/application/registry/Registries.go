package registry

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry/AggregateRegistry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry/ProjectionRegistry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry/TenantRegistry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry/WorkerRegistry"
)

func NewRegistries() *Registries {
	return &Registries{
		AggregateRegistry:  AggregateRegistry.NewRegistry(),
		ProjectionRegistry: ProjectionRegistry.NewRegistry(),
		TenantRegistry:     TenantRegistry.NewRegistry(),
		WorkerRegistry:     WorkerRegistry.NewRegistry(),
	}
}

type Registries struct {
	AggregateRegistry  *AggregateRegistry.Registry
	ProjectionRegistry *ProjectionRegistry.Registry
	TenantRegistry     *TenantRegistry.Registry
	WorkerRegistry     *WorkerRegistry.Registry
}
