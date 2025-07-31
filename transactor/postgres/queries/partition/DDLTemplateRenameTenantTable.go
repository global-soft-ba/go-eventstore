package partition

import (
	"bytes"
	"fmt"
)

const renameTenantTableToRebuildTableDDL = `ALTER TABLE "{{.TenantTable}}" RENAME TO "{{.RebuildTable}}";`

type RenameTenantTableToRebuildTableStatementData struct {
	TenantTable  string
	RebuildTable string
}

func (b *Builder) RenameTenantTableToRebuildTable(tenantID, parentTable string) (statement string, err error) {
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, renameTenantTableToRebuildTableDDL, RenameTenantTableToRebuildTableStatementData{
		TenantTable:  b.GetTenantTableName(tenantID, parentTable),
		RebuildTable: b.GetTempTableName(tenantID, parentTable),
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", renameTenantTableToRebuildTableDDL, err)
	}
	return buf.String(), nil
}
