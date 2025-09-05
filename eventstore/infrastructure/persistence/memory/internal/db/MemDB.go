package db

import (
	"github.com/global-soft-ba/go-eventstore"
	kvTable2 "github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/indexer"
	"github.com/hashicorp/go-memdb"
)

type AutoIncrementEvent struct {
	ID    int64
	Event event.PersistenceEvent
}

func NewInMemoryDB() *MemDB {
	db, err := memdb.NewMemDB(dbSchema)
	if err != nil {
		panic(err)
	}
	return &MemDB{
		MemDB:       db,
		serialInt64: kvTable2.NewKeyValuesTable[int64](),
	}
}

type MemDB struct {
	*memdb.MemDB

	serialInt64 kvTable2.IKVTable[int64]
}

// DBTX is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (m *MemDB) DBTX(write bool) *MemDBTX {
	return &MemDBTX{
		Txn: m.Txn(write),
		db:  m,
	}
}

type MemDBTX struct {
	*memdb.Txn
	db *MemDB
}

func (m *MemDBTX) InsertEventWithAutoIncrement(table string, obj event.PersistenceEvent) error {
	currentNumber, err := kvTable2.GetFirst(m.db.serialInt64, kvTable2.NewKey(table))
	if kvTable2.IsKeyNotFound(err) {
		//init case
		currentNumber = 0
	}

	currentNumber++
	err = m.Txn.Insert(table, AutoIncrementEvent{
		ID:    currentNumber,
		Event: obj,
	})

	if err != nil {
		return err
	}

	err = kvTable2.Set(m.db.serialInt64, kvTable2.NewKey(table), currentNumber)
	if err != nil {
		return err
	}

	return err
}

func (m *MemDBTX) DeleteEventWithAutoIncrement(table string, internalID int64, evt event.PersistenceEvent) error {
	obj := AutoIncrementEvent{
		ID:    internalID,
		Event: evt,
	}

	return m.Txn.Delete(table, obj)
}

const (
	IdxUnique                = "id"
	IdxSetOfId               = "IdxSetOfId"
	IdxTenantIdAggregateType = "IdxTenantIdAggregateType"
	IdxTenantId              = "IdxTenantId"
	IdxUniqueTenantsId       = "idxUniqueTenantId"
	IdxSetOfIdClass          = "IdxSetOfIdClass"
	IdxWithEventID           = "IdxWithEventID"

	TableAggregates       = "aggregates"
	TableEvent            = "event"
	TableSnapShot         = "snapshot"
	TableProjections      = "projections"
	TableProjectionsQueue = "projectionsQueue"
)

var dbSchema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		TableAggregates: {
			Name: TableAggregates,
			Indexes: map[string]*memdb.IndexSchema{
				IdxUnique: {
					Name:   IdxUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "AggregateType"},
							&memdb.StringFieldIndex{Field: "AggregateID"},
						},
						AllowMissing: true,
					},
				},
				//we need this duplicate index, to unify the name of indexes for loader (see other idx below)
				IdxSetOfId: {
					Name:   IdxSetOfId,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "AggregateType"},
							&memdb.StringFieldIndex{Field: "AggregateID"},
						},
						AllowMissing: false,
					},
				},
				IdxTenantIdAggregateType: {
					Name:   IdxTenantIdAggregateType,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "AggregateType"},
						},
						AllowMissing: false,
					},
				},
				IdxTenantId: {
					Name:   IdxTenantId,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
						},
						AllowMissing: false,
					},
				},
				IdxUniqueTenantsId: {
					Name:   IdxUniqueTenantsId,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
						},
						AllowMissing: false,
					},
				},
			},
		},
		TableEvent: {
			Name: TableEvent,
			Indexes: map[string]*memdb.IndexSchema{
				IdxUnique: {
					Name:   IdxUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.IntFieldIndex{Field: "ID"},
						},
						AllowMissing: false,
					},
				},
				IdxTenantId: {
					Name:   IdxTenantId,
					Unique: false,
					Indexer: &indexer.SubFieldIndexer{
						Fields: []indexer.Field{
							{
								Struct: "Event",
								Sub:    "TenantID",
							},
						},
					},
				},
				IdxSetOfId: {
					Name:   IdxSetOfId,
					Unique: false,
					Indexer: &indexer.SubFieldIndexer{
						Fields: []indexer.Field{
							{
								Struct: "Event",
								Sub:    "TenantID",
							}, {
								Struct: "Event",
								Sub:    "AggregateType",
							}, {
								Struct: "Event",
								Sub:    "AggregateID",
							},
						},
					},
					AllowMissing: false,
				},
				IdxSetOfIdClass: {
					Name:   IdxSetOfIdClass,
					Unique: false,
					Indexer: &indexer.SubFieldIndexer{
						Fields: []indexer.Field{
							{
								Struct: "Event",
								Sub:    "TenantID",
							}, {
								Struct: "Event",
								Sub:    "AggregateType",
							}, {
								Struct: "Event",
								Sub:    "AggregateID",
							},
							{
								Struct: "Event",
								Sub:    "Class",
							},
						},
					},
					AllowMissing: false,
				},
				IdxWithEventID: {
					Name:   IdxWithEventID,
					Unique: true,
					Indexer: &indexer.SubFieldIndexer{
						Fields: []indexer.Field{
							{
								Struct: "Event",
								Sub:    "TenantID",
							}, {
								Struct: "Event",
								Sub:    "AggregateType",
							}, {
								Struct: "Event",
								Sub:    "AggregateID",
							},
							{
								Struct: "Event",
								Sub:    "ID",
							},
						},
					},
					AllowMissing: false,
				},
			},
		},
		TableSnapShot: {
			Name: TableSnapShot,
			Indexes: map[string]*memdb.IndexSchema{
				IdxUnique: {
					Name:   IdxUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "AggregateType"},
							&memdb.StringFieldIndex{Field: "AggregateID"},
							&memdb.StringFieldIndex{Field: "ID"},
						},
						AllowMissing: false,
					},
				},
				IdxSetOfId: {
					Name:   IdxSetOfId,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "AggregateType"},
							&memdb.StringFieldIndex{Field: "AggregateID"},
						},
						AllowMissing: false,
					},
				},
			},
		},
		TableProjections: {
			Name: TableProjections,
			Indexes: map[string]*memdb.IndexSchema{
				IdxUnique: {
					Name:   IdxUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "ProjectionID"},
						},
						AllowMissing: false,
					},
				},
				IdxTenantId: {
					Name:   IdxTenantId,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
						},
						AllowMissing: false,
					},
				},
				IdxUniqueTenantsId: {
					Name:   IdxUniqueTenantsId,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
						},
						AllowMissing: false,
					},
				},
			},
		},
		TableProjectionsQueue: {
			Name: TableProjectionsQueue,
			Indexes: map[string]*memdb.IndexSchema{
				IdxUnique: {
					Name:   IdxUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "ID"},
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "ProjectionID"},
						},
						AllowMissing: false,
					},
				},
				IdxSetOfId: {
					Name:   IdxSetOfId,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "TenantID"},
							&memdb.StringFieldIndex{Field: "ProjectionID"},
						},
						AllowMissing: false,
					},
				},
			},
		},
	},
}
