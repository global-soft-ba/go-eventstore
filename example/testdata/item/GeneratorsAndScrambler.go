package item

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/google/uuid"
	"math/rand"
	"testing"
	"time"
)

func GenerateMultiTenant(t *testing.T, amountOfTenants, aggregatesPerTenant int) (out map[string][]FixtureItem, tenantIDs []string) {
	t.Helper()
	out = make(map[string][]FixtureItem)
	for i := 0; i < amountOfTenants; i++ {
		tenantID := uuid.NewString()
		out[tenantID] = append(out[tenantID], GenerateSingleTenant(t, tenantID, aggregatesPerTenant)...)
		tenantIDs = append(tenantIDs, tenantID)
	}

	return out, tenantIDs
}

func GenerateSingleTenant(t *testing.T, tenantID string, amount int) (out []FixtureItem) {
	t.Helper()
	for i := 0; i < amount; i++ {
		itemID := uuid.NewString()
		out = append(out, GenerateFixture(t, tenantID, itemID, fmt.Sprintf("Item-%d", i+1)))
	}
	return out
}

func GenerateFixture(t *testing.T, tenantID, itemID, itemName string) FixtureItem {
	t.Helper()
	return Fixture(t,
		Create(itemID, tenantID),
		Rename(itemName),
	)
}

func Scramble(t *testing.T, input []FixtureItem) (result []event.IEvent) {
	t.Helper()
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	data := buildScramblerArrays(input)
	for len(data) > 0 {
		idx := rng.Intn(len(data))
		result = append(result, data[idx][0])
		data[idx] = data[idx][1:]
		if len(data[idx]) == 0 {
			data = append(data[:idx], data[idx+1:]...)
		}
	}
	return result
}

func buildScramblerArrays(data []FixtureItem) [][]event.IEvent {
	var out [][]event.IEvent
	for _, d := range data {
		out = append(out, d.Events())
	}
	return out
}
