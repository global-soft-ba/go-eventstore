package tenant

import (
	"context"
	"example/infrastructure/writemodel/item/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

func CreateNewTenant(t *testing.T, _ context.Context, dbPool *pgxpool.Pool, tenantID ...string) {
	_, testWriter := postgres.NewAdapterWithWriterForTest(dbPool)

	for _, tID := range tenantID {
		err := testWriter.Execute(t, context.Background(), "CreateTenant", tID)
		if err != nil {
			t.Errorf("error in CreateTenant: %s", err)
		}
	}
}

func DropTenant(t *testing.T, _ context.Context, dbPool *pgxpool.Pool, tenantID ...string) {
	_, testWriter := postgres.NewAdapterWithWriterForTest(dbPool)

	for _, tID := range tenantID {
		err := testWriter.Execute(t, context.Background(), "DropTenant", tID)
		if err != nil {
			t.Errorf("error in DropTenant: %s", err)
		}
	}
}
