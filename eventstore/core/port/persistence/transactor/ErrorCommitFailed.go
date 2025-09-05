package transactor

import "fmt"

func NewErrorCommitFailed(errIn error) *ErrorCommitFailed {
	return &ErrorCommitFailed{
		msg: fmt.Sprintf("commit of transaction failed: %s", errIn),
		err: errIn,
	}

}

type ErrorCommitFailed struct {
	msg string
	err error
}

func (c *ErrorCommitFailed) Error() string {
	return c.msg
}

func (c *ErrorCommitFailed) Unwrap() error {
	return c.err
}

func (c *ErrorCommitFailed) Is(target error) bool {
	_, ok := target.(*ErrorCommitFailed)
	return ok
}
