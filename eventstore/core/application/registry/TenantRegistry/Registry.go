package TenantRegistry

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
)

func NewRegistry() *Registry {
	return &Registry{
		tenants: kvTable.NewKeyValuesTable[string](),
	}
}

type Registry struct {
	tenants kvTable.IKVTable[string]
}

func (r Registry) Exists(tenantID string) bool {
	_, err := kvTable.Get(r.tenants, kvTable.NewKey(tenantID))
	return err == nil
}

func (r Registry) Register(tenantID string) error {
	return kvTable.Add(r.tenants, kvTable.NewKey(tenantID), tenantID)
}
