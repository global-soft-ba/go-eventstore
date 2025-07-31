package transactor

import "fmt"

func NewErrorLockFailed(errIn error) *ErrorLockFailed {
	return &ErrorLockFailed{
		err: errIn,
	}

}

type ErrorLockFailed struct {
	err error
}

func (c *ErrorLockFailed) Error() string {
	return fmt.Sprintf("optimistic lock due to change of keys failed: %s", c.err)
}

func (c *ErrorLockFailed) Unwrap() error {
	return c.err
}

func (c *ErrorLockFailed) Is(target error) bool {
	_, ok := target.(*ErrorLockFailed)
	return ok
}
