package item

import (
	"example/core/domain/model/item"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type FixtureOptions func(this *FixtureItem)

type FixtureItem struct {
	*testing.T

	i item.Item
}

func Item(t *testing.T, options ...FixtureOptions) item.Item {
	t.Helper()
	fixture := &FixtureItem{T: t, i: item.Item{}}
	for _, option := range options {
		option(fixture)
	}

	return fixture.i
}

func Fixture(t *testing.T, options ...FixtureOptions) FixtureItem {
	t.Helper()
	fixture := &FixtureItem{T: t, i: item.Item{}}
	for _, option := range options {
		option(fixture)
	}

	return *fixture
}

func (f FixtureItem) Item() item.Item {
	return f.i
}

func (f FixtureItem) Events() []event.IEvent {
	return f.i.GetUnsavedChanges()
}

func (f FixtureItem) EventsOfTypes(events ...string) (result []event.IEvent) {
	for _, evt := range f.Events() {
		for _, eventType := range events {
			if event.EventType(evt) == eventType {
				result = append(result, evt)
			}
		}
	}
	return result
}

func Create(itemID, tenantID string) FixtureOptions {
	return func(this *FixtureItem) {
		i, err := item.CreateItem(itemID, tenantID)
		assert.Nil(this.T, err)
		this.i = i
	}
}

func Rename(name string) FixtureOptions {
	return func(this *FixtureItem) {
		i, err := this.i.Rename(name, time.Time{})
		assert.Nil(this.T, err)
		this.i = i
	}
}

func Delete() FixtureOptions {
	return func(this *FixtureItem) {
		i, err := this.i.Delete()
		assert.Nil(this.T, err)
		this.i = i
	}
}
