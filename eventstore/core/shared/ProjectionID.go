package shared

func NewProjectionID(tenantID, projectionID string) ProjectionID {
	return ProjectionID{
		TenantID:     tenantID,
		ProjectionID: projectionID,
	}
}

func NewProjectionIDs(tenantID string, projectionIDs ...string) []ProjectionID {
	var projIDs []ProjectionID
	for _, id := range projectionIDs {
		projIDs = append(projIDs, ProjectionID{
			TenantID:     tenantID,
			ProjectionID: id})
	}
	return projIDs
}

type ProjectionID struct {
	TenantID     string
	ProjectionID string
}
