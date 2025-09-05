package transactor

import (
	"fmt"
)

func NewErrorTenantWasNotCreated(tenantID string, errIn error) *ErrorTenantWasNotCreated {
	return &ErrorTenantWasNotCreated{
		tenantID: tenantID,
		err:      errIn,
	}
}

type ErrorTenantWasNotCreated struct {
	tenantID string
	err      error
}

func (c *ErrorTenantWasNotCreated) Error() string {
	return fmt.Sprintf("tenant %q was not created yet: %s", c.tenantID, c.err)
}

func (c *ErrorTenantWasNotCreated) Unwrap() error {
	return c.err
}

func (c *ErrorTenantWasNotCreated) Is(target error) bool {
	t, ok := target.(*ErrorTenantWasNotCreated)
	return ok && t.tenantID == c.tenantID
}
