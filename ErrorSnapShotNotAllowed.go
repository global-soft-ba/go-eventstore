package event

import (
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewErrorSnapShotNotAllowed(errIn error, id shared.AggregateID) *ErrorSnapShotNotAllowed {
	return &ErrorSnapShotNotAllowed{
		AggregateID: id,
		err:         errIn,
	}
}

type ErrorSnapShotNotAllowed struct {
	AggregateID shared.AggregateID
	err         error
}

func (c *ErrorSnapShotNotAllowed) Error() string {
	return fmt.Sprintf("snapshot failed: aggregate %q does not have the most latest version: %v", c.AggregateID, c.err)
}

func (c *ErrorSnapShotNotAllowed) Unwrap() error {
	return c.err
}

func (c *ErrorSnapShotNotAllowed) Is(target error) bool {
	if err, ok := target.(*ErrorSnapShotNotAllowed); ok {
		return c.AggregateID == err.AggregateID && errors.Is(c.err, err.err)
	}
	return false
}
