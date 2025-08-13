package tables

const (
	// ItemTableName is the name of the table for items.
	ItemTableName = "items"

	ColID       = "id"
	ColTenantID = "tenant_id"
	ColItemID   = "item_id"
	ColItemName = "item_name"
)

var ItemsTable = ItemsTableSchema{
	TableName: ItemTableName,

	ID:       ColID,
	TenantID: ColTenantID,
	ItemID:   ColItemID,
	ItemName: ColItemName,
}

type ItemsTableSchema struct {
	TableName string

	ID       string
	TenantID string
	ItemID   string
	ItemName string
}

func (i ItemsTableSchema) AllReadColumns() []string {
	return []string{
		i.ID,
		i.TenantID,
		i.ItemID,
		i.ItemName,
	}
}

func (i ItemsTableSchema) AllInsertColumns() []string {
	return []string{
		i.TenantID,
		i.ItemID,
		i.ItemName,
	}
}

type ItemRow struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	ItemID   string `db:"item_id"`
	ItemName string `db:"item_name"`
}
