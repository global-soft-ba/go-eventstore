package event

import (
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"slices"
)

func NewErrorProjectionOutOfSync(errIn error, id ...shared.ProjectionID) *ErrorProjectionOutOfSync {
	return &ErrorProjectionOutOfSync{
		ID:  id,
		err: errIn,
	}
}

type ErrorProjectionOutOfSync struct {
	ID  []shared.ProjectionID
	err error
}

func (c *ErrorProjectionOutOfSync) Error() string {
	return fmt.Sprintf("projection %v is out of sync: %v", c.ID, c.err)
}

func (c *ErrorProjectionOutOfSync) Unwrap() error {
	return c.err
}

func (c *ErrorProjectionOutOfSync) Is(target error) bool {
	if err, ok := target.(*ErrorProjectionOutOfSync); ok {
		return slices.Equal(c.ID, err.ID) && errors.Is(c.err, err.err)
	}
	return false
}
