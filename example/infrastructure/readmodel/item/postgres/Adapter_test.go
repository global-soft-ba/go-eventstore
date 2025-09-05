//go:build integration

package postgres

import (
	"context"
	env "example/core/port/environment"
	readModelPort "example/core/port/readmodel/item"
	"example/infrastructure/environment"
	"example/infrastructure/persistence"
	"example/infrastructure/persistence/postgres/tables"
	"example/infrastructure/readmodel/item/postgres/queries"
	"example/infrastructure/writemodel/item/postgres"
	"example/presentation/paginator"
	"example/testdata/item"
	"example/testdata/tenant"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	postgresPersistence "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres"
	transactorPort "github.com/global-soft-ba/go-eventstore/transactor"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactorForTest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
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

func _TestPersistent() transactorPort.Port {
	return transactor.NewTransactor(dbPool)
}

func _ReadModelAdapterForTest() readModelPort.Port {
	return &Adapter{
		trans: transactorForTest.NewTxPassThroughTransactor(),
		sql:   queries.NewSqlBuilder(tables.DatabaseSchema, sq.Dollar),
	}
}

var (
	fixedItemID   = "d464a012-83b1-4ec2-83a9-75f66c81b251"
	fixedItemName = "Example Item"
)

func fixedFixture(t *testing.T, tenantID string) []event.IEvent {
	i := item.Fixture(t,
		item.Create(fixedItemID, tenantID),
		item.Rename(fixedItemName),
	)
	return i.Events()
}

func seedData(t *testing.T, cnt int, tID string, generateID func(i int) string, generateRef func(i int) string) []event.IEvent {
	var resultEvents []event.IEvent
	for i := 0; i < cnt; i++ {
		f := item.GenerateFixture(t, tID, generateID(i), generateRef(i))
		resultEvents = append(resultEvents, f.Events()...)
	}
	return resultEvents
}

func TestAdapter_FindItems(t *testing.T) {
	tID := uuid.NewString()
	tenant.CreateNewTenant(t, context.Background(), dbPool, tID)
	defer tenant.DropTenant(t, context.Background(), dbPool, tID)

	type args struct {
		ctx      context.Context
		pageSize uint
		fields   []event.SortField
	}

	tests := []struct {
		name               string
		args               args
		seedCount          int
		searchFields       []event.SearchField
		wantedPage         int
		wantedOnPage       []string
		assertErr          assert.ErrorAssertionFunc
		assertFixedFixture bool
	}{
		{
			name: "retrieve all result (no pagination)",
			args: args{
				ctx:      context.Background(),
				pageSize: 0,
				fields:   nil,
			},
			seedCount:    5,
			searchFields: []event.SearchField{{paginator.SearchAny, "#", "="}},
			wantedPage:   1,
			wantedOnPage: []string{"#", "##", "###", "####", "#####"},
			assertErr:    assert.NoError,
		},
		{
			name: "retrieve single result (single page)",
			args: args{
				ctx:      context.Background(),
				pageSize: 1,
				fields:   []event.SortField{{Name: readModelPort.SortItemName, IsDesc: false}},
			},
			seedCount:    5,
			searchFields: []event.SearchField{{paginator.SearchAny, "#", "="}},
			wantedPage:   5,
			wantedOnPage: []string{"#####"},
			assertErr:    assert.NoError,
		},
		{
			name: "retrieve single result (double page)",
			args: args{
				ctx:      context.Background(),
				pageSize: 2,
				fields:   []event.SortField{{Name: readModelPort.SortItemName, IsDesc: false}},
			},
			seedCount:    5,
			searchFields: []event.SearchField{{paginator.SearchAny, "##", "="}},
			wantedPage:   2,
			wantedOnPage: []string{"####", "#####"},
			assertErr:    assert.NoError,
		},
		{
			name: "retrieve single result (no pagination)",
			args: args{
				ctx:      context.Background(),
				pageSize: 0,
				fields:   nil,
			},
			seedCount:    5,
			searchFields: []event.SearchField{{paginator.SearchAny, "####", "="}},
			wantedPage:   1,
			wantedOnPage: []string{"####", "#####"},
			assertErr:    assert.NoError,
		},
		{
			name: "retrieve no result (wrong name)",
			args: args{
				ctx:      context.Background(),
				pageSize: 0,
				fields:   nil,
			},
			seedCount:    5,
			searchFields: []event.SearchField{{readModelPort.SearchItemName, "1", "="}},
			wantedPage:   1,
			wantedOnPage: nil,
			assertErr:    assert.NoError,
		},
		{
			name: "retrieve single result (multiple search fields)",
			args: args{
				ctx:      context.Background(),
				pageSize: 0,
				fields:   nil,
			},
			seedCount: 5,
			searchFields: []event.SearchField{
				{readModelPort.SearchItemID, fixedItemID, "="},
				{readModelPort.SearchItemName, fixedItemName, "="},
			},
			wantedPage:         1,
			wantedOnPage:       []string{fixedItemName},
			assertErr:          assert.NoError,
			assertFixedFixture: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := _ReadModelAdapterForTest()
			_, itemWriter := postgres.NewTxPassThroughAdapterWithWriterForTest()
			_ = _TestPersistent().ExecWithinTransaction(tt.args.ctx, func(txCtx context.Context) error {
				// seed
				events := seedData(t, tt.seedCount, tID, func(i int) string { return uuid.NewString() }, func(i int) string { return strings.Repeat("#", i+1) })
				if tt.assertFixedFixture {
					events = append(events, fixedFixture(t, tID)...)
				}
				err := itemWriter.Write(t, txCtx, events...)
				assert.NoError(t, err)

				//navigate to  page
				var got []readModelPort.DTO
				// first page
				got, pages, err := adp.FindItems(txCtx, tID, event.PageDTO{PageSize: tt.args.pageSize, SearchFields: tt.searchFields, SortFields: tt.args.fields})
				// second page and follows
				for i := 1; i < tt.wantedPage; i++ {
					got, pages, err = adp.FindItems(txCtx, tID, pages.Next)
					tt.assertErr(t, err)
				}
				item.AssertCorrectOrderOfItems(t, tt.wantedOnPage, got)

				//tear down test data (rollback)
				return postgresPersistence.Rollback
			})
		})
	}
}

func Test_FindItemByID(t *testing.T) {
	t.Parallel()
	tenantID := uuid.NewString()
	tenant.CreateNewTenant(t, context.Background(), dbPool, tenantID)
	defer tenant.DropTenant(t, context.Background(), dbPool, tenantID)

	itemID1 := uuid.NewString()
	itemID2 := uuid.NewString()
	itemID3 := uuid.NewString()

	tests := []struct {
		name            string
		ctx             context.Context
		wantItemWithID  string
		wantErr         bool
		wantNotFoundErr bool
		data            func(t *testing.T, txCtx context.Context) []item.FixtureItem
	}{
		{
			name:           "find with existing ID",
			ctx:            context.Background(),
			wantItemWithID: itemID1,
			wantErr:        false,
			data: func(t *testing.T, txCtx context.Context) (fixtures []item.FixtureItem) {
				fixtures = append(fixtures,
					item.GenerateFixture(t, tenantID, itemID1, "Example Item 1"),
					item.GenerateFixture(t, tenantID, itemID2, "Example Item 2"),
					item.GenerateFixture(t, tenantID, itemID3, "Example Item 3"),
				)
				return fixtures
			},
		},
		{
			name:            "find with unknown ID",
			ctx:             context.Background(),
			wantItemWithID:  uuid.NewString(),
			wantErr:         true,
			wantNotFoundErr: true,
			data: func(t *testing.T, txCtx context.Context) (fixtures []item.FixtureItem) {
				fixtures = append(fixtures,
					item.GenerateFixture(t, tenantID, itemID1, "Example Item 1"),
					item.GenerateFixture(t, tenantID, itemID2, "Example Item 2"),
					item.GenerateFixture(t, tenantID, itemID3, "Example Item 3"),
				)
				return fixtures
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := _ReadModelAdapterForTest()
			_, writer := postgres.NewTxPassThroughAdapterWithWriterForTest()
			_ = _TestPersistent().ExecWithinTransaction(tt.ctx, func(txCtx context.Context) error {

				fixtures := tt.data(t, tt.ctx)
				for _, f := range fixtures {
					err := writer.Write(t, txCtx, f.Events()...)
					if err != nil {
						panic(err)
					}
					assert.NoError(t, err)
				}

				itemDTO, err := adp.FindItemByID(txCtx, tenantID, tt.wantItemWithID)
				if (err != nil) != tt.wantErr {
					t.Errorf("FindItemByID() error = %v, wantErr %v", err, tt.wantErr)
					return fmt.Errorf("FindItemByID() error = %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					assert.Empty(t, itemDTO)
				}
				return postgresPersistence.Rollback
			})
		})
	}
}
