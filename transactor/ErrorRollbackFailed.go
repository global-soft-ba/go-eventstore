package transactor

import (
	"fmt"
)

func NewErrorRollbackFailed(errIn error) *ErrorRollbackFailed {
	return &ErrorRollbackFailed{
		err: errIn,
	}
}

type ErrorRollbackFailed struct {
	err error
}

func (c *ErrorRollbackFailed) Error() string {
	return fmt.Sprintf("rollback of transaction failed: %s", c.err)
}

func (c *ErrorRollbackFailed) Unwrap() error {
	return c.err
}

func (c *ErrorRollbackFailed) Is(target error) bool {
	_, ok := target.(*ErrorRollbackFailed)
	return ok
}
