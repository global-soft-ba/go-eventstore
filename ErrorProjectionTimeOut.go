package event

import (
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewErrorProjectionTimeOut(errIn error, id shared.ProjectionID) *ErrorProjectionTimeOut {
	return &ErrorProjectionTimeOut{
		ID:  id,
		err: errIn,
	}
}

type ErrorProjectionTimeOut struct {
	ID  shared.ProjectionID
	err error
}

func (c *ErrorProjectionTimeOut) Error() string {
	return fmt.Sprintf("timeout in projection %q: %v", c.ID, c.err)
}

func (c *ErrorProjectionTimeOut) Unwrap() error {
	return c.err
}

func (c *ErrorProjectionTimeOut) Is(target error) bool {
	if err, ok := target.(*ErrorProjectionTimeOut); ok {
		return c.ID == err.ID && errors.Is(c.err, err.err)
	}
	return false
}
