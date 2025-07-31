package event

import (
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewErrorProjectionInWrongState(errIn error, got, want string, id shared.ProjectionID) *ErrorProjectionInWrongState {
	return &ErrorProjectionInWrongState{
		ID:        id,
		State:     got,
		WantState: want,
		err:       errIn,
	}
}

type ErrorProjectionInWrongState struct {
	ID        shared.ProjectionID
	State     string
	WantState string
	msg       string
	err       error
}

func (c *ErrorProjectionInWrongState) Error() string {
	return fmt.Sprintf("wrong state %q of projection %q (want %q): %v", c.State, c.ID, c.WantState, c.err)
}

func (c *ErrorProjectionInWrongState) Unwrap() error {
	return c.err
}

func (c *ErrorProjectionInWrongState) Is(target error) bool {
	if err, ok := target.(*ErrorProjectionInWrongState); ok {
		return c.ID == err.ID && c.State == err.State && c.WantState == err.WantState && c.msg == err.msg && errors.Is(c.err, err.err)
	}
	return false
}
