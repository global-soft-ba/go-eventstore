package item

import (
	"example/core/port/readmodel/item"
	"github.com/go-playground/assert/v2"
	"testing"
)

func AssertCorrectOrderOfItems(t *testing.T, want []string, got []item.DTO) {
	t.Helper()

	var names []string
	for _, i := range got {
		names = append(names, i.ItemName)
	}
	assert.Equal(t, want, names)
}
