package queries

import (
	"example/infrastructure/persistence/postgres/tables"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries/partition"
)

func NewSqlBuilder(dataBaseSchema string, placeHolder sq.PlaceholderFormat) (*Builder, error) {
	partitionBuilder, err := partition.NewSqlBuilder(dataBaseSchema, placeHolder)
	if err != nil {
		return nil, err
	}
	builder := Builder{
		Builder: partitionBuilder,
	}
	return &builder, nil
}

type Builder struct {
	*partition.Builder
}

func (b *Builder) CreateItem(row tables.ItemRow) (string, []interface{}, error) {
	onConflict := queries.OnConflict{tables.ColTenantID, tables.ColItemID}
	onConflictSuffix := onConflict.DoUpdateOrNothing()

	return b.Build().
		Insert(b.GetEscapedTenantTableName(row.TenantID, tables.ItemTableName)).
		Columns(tables.ItemsTable.AllInsertColumns()...).
		Values(
			row.TenantID,
			row.ItemID,
			row.ItemName,
		).
		Suffix(onConflictSuffix.WithReturningOrNothing()).
		ToSql()
}

func (b *Builder) DeleteItem(tenantID, itemID string) (string, []interface{}, error) {
	return b.Build().
		Delete(b.GetEscapedTenantTableName(tenantID, tables.ItemTableName)).
		Where(sq.Eq{
			tables.ColTenantID: tenantID,
			tables.ColItemID:   itemID,
		}).
		ToSql()
}

func (b *Builder) RenameItem(tenantID, itemID, name string) (string, []interface{}, error) {
	return b.Build().
		Update(b.GetEscapedTenantTableName(tenantID, tables.ItemTableName)).
		Set(tables.ColItemName, name).
		Where(sq.Eq{
			tables.ColTenantID: tenantID,
			tables.ColItemID:   itemID,
		}).
		ToSql()
}

// -----------------------------------------------------------------------------------------------------------------------
// --------------------------------------------TEST SUPPORT------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------

func (b *Builder) ForTestGetAllItems(tenantID string) (string, []interface{}, error) {
	table := tables.ItemsTable
	tableName := tables.ItemsTable.TableName
	query := b.Build().
		Select(table.AllReadColumns()...).
		From(tableName).
		Where(sq.Eq{
			table.TenantID: tenantID,
		})
	return query.ToSql()
}
