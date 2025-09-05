//go:build integration

package postgres

import (
	"context"
	env "example/core/port/environment"
	"example/infrastructure/environment"
	"example/infrastructure/persistence"
	"example/infrastructure/persistence/postgres/tables"
	"example/testdata"
	"example/testdata/item"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

var (
	dbPool *pgxpool.Pool
	err    error
)

func TestMain(m *testing.M) {
	environment.InitEnvironment(env.Test)
	config := env.Config()
	dbPool, err = persistence.NewPgxClient(context.Background(), persistence.Options{
		Host:           config.DatabaseHost,
		Port:           config.DatabasePort,
		Username:       config.DatabaseUser,
		Password:       config.DatabasePassword,
		Schema:         config.DatabaseSchema,
		MaxConnections: config.DatabaseMaxNumberOfConnections,
		AutoMigrate:    true,
	})
	if err != nil {
		panic(err)
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}

func Test_CRUD_Postgres(t *testing.T) {
	tenantID1 := uuid.NewString()
	tenantID2 := uuid.NewString()
	tenantID3 := uuid.NewString()

	itemID1 := uuid.NewString()
	itemID2 := uuid.NewString()
	itemID3 := uuid.NewString()

	tests := []struct {
		name          string
		ctx           context.Context
		chunkSize     int
		wantedTenants []string
		data          func(t *testing.T, txCtx context.Context) []item.FixtureItem
	}{
		{
			name:          "test creation",
			ctx:           context.Background(),
			chunkSize:     6,
			wantedTenants: []string{tenantID1},
			data: func(t *testing.T, txCtx context.Context) (fixtures []item.FixtureItem) {
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID1, tenantID1),
					item.Rename("New Item Name 1"),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID2, tenantID1),
					item.Rename("New Item Name 2"),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID3, tenantID1),
					item.Rename("New Item Name 3"),
				))
				return fixtures
			},
		},
		{
			name:          "test deletion",
			ctx:           context.Background(),
			chunkSize:     6,
			wantedTenants: []string{tenantID1},
			data: func(t *testing.T, txCtx context.Context) (fixtures []item.FixtureItem) {
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID1, tenantID1),
					item.Rename("New Item Name 1"),
					item.Delete(),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID2, tenantID1),
					item.Rename("New Item Name 2"),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID3, tenantID1),
					item.Rename("New Item Name 3"),
				))
				return fixtures
			},
		},
		{
			name:          "test creating items for different tenants",
			ctx:           context.Background(),
			chunkSize:     6,
			wantedTenants: []string{tenantID1, tenantID2, tenantID3},
			data: func(t *testing.T, txCtx context.Context) (fixtures []item.FixtureItem) {
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID1, tenantID1),
					item.Rename("New Item Name 1"),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID2, tenantID2),
					item.Rename("New Item Name 2"),
				))
				fixtures = append(fixtures, item.Fixture(t,
					item.Create(itemID3, tenantID3),
					item.Rename("New Item Name 3"),
				))
				return fixtures
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer dropTenants(t, context.Background(), tt.wantedTenants, dbPool)
			adp, testWriter := NewAdapterWithWriterForTest(dbPool)

			fixtures := tt.data(t, tt.ctx)
			for _, f := range fixtures {
				err := testWriter.Write(t, tt.ctx, f.Events()...)
				if err != nil {
					panic(err)
				}
				assert.NoError(t, err)
			}
			errTx := adp.(*adapter).Transactor().ExecWithinTransaction(context.Background(), func(txCtx context.Context) (err error) {
				var itemRows []tables.ItemRow
				for _, tenant := range tt.wantedTenants {
					res, err := forTestGetAllItemRows(txCtx, adp.(*adapter), tenant)
					if err != nil {
						panic(err)
					}
					assert.Nil(t, err)
					itemRows = append(itemRows, res...)
				}
				AssertEqualItemRows(t, mapFixturesToItemRows(fixtures), itemRows)
				return nil
			})
			if errTx != nil {
				panic(errTx)
			}
			assert.Nil(t, errTx)
		})
	}
}

func Test_Rebuild_Postgres(t *testing.T) {
	tests := []struct {
		name              string
		ctx               context.Context
		dataCounter       int
		prefillPercentage int // 0 no prefill 100 all in prefill
		data              func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string)
		executeRebuild    func(t *testing.T, txCtx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error
		createErrorState  bool
		assertErr         assert.ErrorAssertionFunc
	}{
		{
			name:              "rebuild single tenant for existing tenant (empty case)",
			ctx:               context.Background(),
			dataCounter:       5,
			prefillPercentage: 0,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				tID := uuid.NewString()
				fix, tenantIDs := item.GenerateSingleTenant(t, tID, cnt), []string{tID}
				return map[string][]item.FixtureItem{tID: fix}, tenantIDs
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
		},
		{
			name:              "rebuild multi tenant for existing tenant and data (empty case)",
			ctx:               context.Background(),
			dataCounter:       10,
			prefillPercentage: 0,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				return item.GenerateMultiTenant(t, cnt, cnt)
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
		},
		{
			name:              "rebuild single tenant for existing tenant and data (standard case)",
			ctx:               context.Background(),
			dataCounter:       5,
			prefillPercentage: 20,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				tID := uuid.NewString()
				fix, tenantIDs := item.GenerateSingleTenant(t, tID, cnt), []string{tID}
				return map[string][]item.FixtureItem{tID: fix}, tenantIDs
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
		},
		{
			name:              "rebuild single tenant for existing tenant and data (scan size = 1)",
			ctx:               context.Background(),
			dataCounter:       5,
			prefillPercentage: 20,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				tID := uuid.NewString()
				fix, tenantIDs := item.GenerateSingleTenant(t, tID, cnt), []string{tID}
				return map[string][]item.FixtureItem{tID: fix}, tenantIDs
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
		},
		{
			name:              "rebuild multi tenant for existing tenant and data (standard case)",
			ctx:               context.Background(),
			dataCounter:       10,
			prefillPercentage: 20,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				return item.GenerateMultiTenant(t, cnt, cnt)
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
		},
		{
			name:              "rebuild single tenant for existing tenant and data (from error state)",
			ctx:               context.Background(),
			dataCounter:       5,
			prefillPercentage: 20,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				tID := uuid.NewString()
				fix, tenantIDs := item.GenerateSingleTenant(t, tID, cnt), []string{tID}
				return map[string][]item.FixtureItem{tID: fix}, tenantIDs
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				err := writer.Write(t, ctx, data...)
				assert.Nil(t, err)
				return nil
			},
			createErrorState: true,
		},
		{
			name:              "rebuild multi tenant for existing tenant and data (from error state)",
			ctx:               context.Background(),
			dataCounter:       10,
			prefillPercentage: 20,
			data: func(t *testing.T, cnt int) (map[string][]item.FixtureItem, []string) {
				return item.GenerateMultiTenant(t, cnt, cnt)
			},
			executeRebuild: func(t *testing.T, ctx context.Context, writer testdata.IWriter[event.IEvent, string], data []event.IEvent) error {
				return writer.Write(t, ctx, data...)
			},
			createErrorState: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, tenantIDs := tt.data(t, tt.dataCounter)

			var wg = &sync.WaitGroup{}
			for _, tenantID := range tenantIDs {
				// for each tenant in concurrent go routine
				wg.Add(1)
				go func(tID string) {
					defer wg.Done()
					adp, testWriter := NewAdapterWithWriterForTest(dbPool)
					// ---------------------prefill -----------------------------
					testData := data[tID][:getPrefillRatioEnd(len(data[tID]), tt.prefillPercentage)]
					err := testWriter.Write(t, context.Background(), item.Scramble(t, testData)...)
					assert.Nil(t, err)

					// ---------------------error state provocation-----------------------------
					if tt.createErrorState {
						err = testWriter.Execute(t, context.Background(), "PrepareRebuild", tID)
						assert.Nil(t, err)
					}

					// ---------------------prepare rebuild-----------------------------
					err = testWriter.Execute(t, context.Background(), "PrepareRebuild", tID)
					assert.Nil(t, err)

					// assert
					_ = adp.(*adapter).Transactor().ExecWithoutTransaction(tt.ctx, func(txCtx context.Context) (err error) {
						itemRows, err := forTestGetAllItemRows(txCtx, adp.(*adapter), tID)
						assert.Nil(t, err)
						assert.Equal(t, getPrefillRatioEnd(tt.dataCounter, tt.prefillPercentage), len(itemRows))
						return nil
					})

					// ---------------------execute rebuild-----------------------------
					testDataAll := item.Scramble(t, data[tID])
					err = tt.executeRebuild(t, context.Background(), testWriter, testDataAll)
					assert.Nil(t, err)

					// ---------------------finish rebuild-----------------------------
					err = testWriter.Execute(t, context.Background(), "FinishRebuild", tID)
					assert.Nil(t, err)

					// assert
					errTx := adp.(*adapter).Transactor().ExecWithoutTransaction(context.Background(), func(txCtx context.Context) (err error) {
						// read from write models
						rows, err := forTestGetAllItemRows(txCtx, adp.(*adapter), tID)
						assert.Nil(t, err)
						AssertEqualItemRows(t, mapFixturesToItemRows(data[tID]), rows)
						return nil
					})
					assert.Nil(t, errTx)

					dropTenant(t, context.Background(), tID, dbPool)
				}(tenantID)
			}
			wg.Wait()
		})
	}
}

// ------------------------------------------------------------------
// ---------------------helper functions-----------------------------
// ------------------------------------------------------------------

func dropTenant(t *testing.T, ctx context.Context, tenantID string, dbPool *pgxpool.Pool) {
	_, testWriter := NewAdapterWithWriterForTest(dbPool)
	err := testWriter.Execute(t, context.Background(), "DropTenant", tenantID)
	if err != nil {
		t.Errorf("error in DropTenant: %s", err)
	}
	return
}

func dropTenants(t *testing.T, ctx context.Context, tenantIDs []string, dbPool *pgxpool.Pool) {
	for _, tID := range tenantIDs {
		dropTenant(t, ctx, tID, dbPool)
	}
}

func forTestGetAllItemRows(txCtx context.Context, a *adapter, tenantID string) ([]tables.ItemRow, error) {
	var rows []tables.ItemRow

	stmt, args, err := a.sql.ForTestGetAllItems(tenantID)
	if err != nil {
		return nil, err
	}
	tx, err := a.GetTx(txCtx)
	if err != nil {
		return nil, err
	}

	err = pgxscan.Select(txCtx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func mapFixturesToItemRows(fixtures []item.FixtureItem) []tables.ItemRow {
	var rows []tables.ItemRow
	for _, f := range fixtures {
		i := f.Item()

		if i.IsDeleted() {
			continue // skip deleted items
		}

		rows = append(rows, tables.ItemRow{
			TenantID: i.GetTenantID(),
			ItemID:   i.GetID(),
			ItemName: i.GetName(),
		})
	}
	return rows
}

func AssertEqualItemRows(t *testing.T, expectedRows, gotRows []tables.ItemRow) {
	rowNum := len(expectedRows)
	assert.Equal(t, rowNum, len(gotRows))
	rowCounter := 0
	for _, eRow := range expectedRows {
		for _, gRow := range gotRows {
			if rowsIdentical(eRow, gRow) {
				rowCounter++
			}
		}
	}
}

func rowsIdentical(row1, row2 tables.ItemRow) bool {
	return row1.TenantID == row2.TenantID &&
		row1.ItemID == row2.ItemID &&
		row1.ItemName == row2.ItemName
}

func getPrefillRatioEnd(len int, ratio int) int {
	if ratio == 0 {
		return 0
	}
	if ratio > 100 {
		return len
	}
	return len * (ratio / 100)
}
