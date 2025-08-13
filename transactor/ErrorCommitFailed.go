package transactor

import (
	"fmt"
)

func NewErrorCommitFailed(errIn error) *ErrorCommitFailed {
	return &ErrorCommitFailed{
		err: errIn,
	}

}

type ErrorCommitFailed struct {
	err error
}

func (c *ErrorCommitFailed) Error() string {
	return fmt.Sprintf("commit of transaction failed: %s", c.err)
}

func (c *ErrorCommitFailed) Unwrap() error {
	return c.err
}

func (c *ErrorCommitFailed) Is(target error) bool {
	_, ok := target.(*ErrorCommitFailed)
	return ok
}
