package testdata

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var defaultAssertIEventOptions = []AssertIEventOption{
	CheckAggregateID,
	CheckTenantID,
	CheckClass,
	CheckMigration,
	CheckTransactionTime,
	CheckValidTime,
}

type AssertIEventOption func(t *testing.T, got, want event.IEvent)

func CheckAggregateID(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetAggregateID(), want.GetAggregateID(), "assert aggregateID failed: got %v, want %v", got.GetAggregateID(), want.GetAggregateID())
}

func CheckTenantID(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetTenantID(), want.GetTenantID(), "assert tenantID failed: got %v, want %v", got.GetTenantID(), want.GetTenantID())
}

func CheckClass(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetClass(), want.GetClass(), "assert class failed: got %v, want %v", got.GetClass(), want.GetClass())
}

func CheckMigration(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetMigration(), want.GetMigration(), "assert migration failed: got %v, want %v", got.GetMigration(), want.GetMigration())
}

func CheckTransactionTime(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetTransactionTime(), want.GetTransactionTime(), "assert transaction time failed: got %v, want %v", got.GetTransactionTime(), want.GetTransactionTime())
}

func CheckValidTime(t *testing.T, got, want event.IEvent) {
	assert.Equalf(t, got.GetValidTime(), want.GetValidTime(), "assert valid time failed: got %v, want %v", got.GetValidTime(), want.GetValidTime())
}

func AssertEqualEventStreams(t *testing.T, got, want []event.EventStream, opts ...AssertIEventOption) {
	assert.Equalf(t, len(got), len(want), "assert length failed: got %v, want %v", len(got), len(want))

	for idx := range want {
		AssertEqualStream(t, got[idx].Stream, want[idx].Stream, opts...)
		assert.Equalf(t, got[idx].Version, want[idx].Version, "assert version failed: got %v, want %v", got[idx].Version, want[idx].Version)
	}
}

func AssertEqualStream(t *testing.T, got, want []event.IEvent, opts ...AssertIEventOption) {
	require.Equal(t, len(want), len(got), "require stream length failed: got %v, want %v", len(got), len(want))

	if len(opts) == 0 {
		opts = defaultAssertIEventOptions
	}
	for idx, evt := range want {
		assertIEvent(t, got[idx], evt, opts...)
	}
}

func assertIEvent(t *testing.T, got, want event.IEvent, opts ...AssertIEventOption) {
	for _, opt := range opts {
		opt(t, got, want)
	}
}
