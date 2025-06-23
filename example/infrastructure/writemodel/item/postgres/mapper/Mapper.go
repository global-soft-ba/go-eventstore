package mapper

import (
	"example/core/port/writemodel/item"
	"example/infrastructure/persistence/postgres/tables"
)

func ToItemRow(dto item.DTO) tables.ItemRow {
	return tables.ItemRow{
		TenantID: dto.TenantID,
		ItemID:   dto.ItemID,
		ItemName: dto.ItemName,
	}
}
