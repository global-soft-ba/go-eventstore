package event

import "fmt"

// ErrorConcurrentProjectionAccess used for projections already in execution
type ErrorConcurrentProjectionAccess struct {
	TenantID     string
	ProjectionID string
}

func (c *ErrorConcurrentProjectionAccess) Error() string {
	return fmt.Sprintf("concurrent projection request detected for projection %q of tenant %q", c.ProjectionID, c.TenantID)
}
