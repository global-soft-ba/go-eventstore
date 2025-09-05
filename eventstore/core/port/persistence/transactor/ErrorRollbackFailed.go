package transactor

import (
	"fmt"
)

func NewErrorRollbackFailed(errIn error) *ErrorRollbackFailed {
	return &ErrorRollbackFailed{
		msg: fmt.Sprintf("rollback of transaction failed:%s", errIn),
		err: errIn,
	}
}

type ErrorRollbackFailed struct {
	msg string
	err error
}

func (c *ErrorRollbackFailed) Error() string {
	return c.msg
}

func (c *ErrorRollbackFailed) Unwrap() error {
	return c.err
}

func (c *ErrorRollbackFailed) Is(target error) bool {
	_, ok := target.(*ErrorRollbackFailed)
	return ok
}
