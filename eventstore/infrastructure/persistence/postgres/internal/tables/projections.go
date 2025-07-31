package tables

import "time"

type ProjectionsRow struct {
	TenantID     string    `db:"tenant_id"`
	ProjectionID string    `db:"projection_id"`
	State        string    `db:"state"`
	UpdatedAt    time.Time `db:"updated_at"`
}

var ProjectionsTable = ProjectionsTableSchema{
	Name:         "projections",
	TenantID:     "tenant_id",
	ProjectionID: "projection_id",
	State:        "state",
	UpdatedAt:    "updated_at",
}

type ProjectionsTableSchema struct {
	Name string

	TenantID     string
	ProjectionID string
	State        string
	UpdatedAt    string
}

func (a ProjectionsTableSchema) AllColumns() []string {
	return []string{a.TenantID, a.ProjectionID, a.State, a.UpdatedAt}
}

func (a ProjectionsTableSchema) AllInsertColumns() []string {
	return []string{a.TenantID, a.ProjectionID, a.State}
}
