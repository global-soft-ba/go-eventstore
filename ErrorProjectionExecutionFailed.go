package event

import (
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewErrorProjectionExecutionFailed(errIn error, id shared.ProjectionID) *ErrorProjectionExecutionFailed {
	return &ErrorProjectionExecutionFailed{
		ID:  id,
		err: errIn,
	}
}

type ErrorProjectionExecutionFailed struct {
	ID  shared.ProjectionID
	err error
}

func (c *ErrorProjectionExecutionFailed) Error() string {
	return fmt.Sprintf("projection %q failed: %v", c.ID, c.err)
}

func (c *ErrorProjectionExecutionFailed) Unwrap() error {
	return c.err
}

func (c *ErrorProjectionExecutionFailed) Is(target error) bool {
	if err, ok := target.(*ErrorProjectionExecutionFailed); ok {
		return c.ID == err.ID && errors.Is(c.err, err.err)
	}
	return false
}
