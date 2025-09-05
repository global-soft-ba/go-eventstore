package partition

import (
	"bytes"
	"fmt"
)

const truncateRebuildTableDDL = `TRUNCATE TABLE "{{.RebuildTable}}";`

type TruncateRebuildTableStatementData struct {
	RebuildTable string
}

func (b *Builder) TruncateTenantTable(tenantID, parentTable string) (statement string, err error) {
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, truncateRebuildTableDDL, TruncateRebuildTableStatementData{
		RebuildTable: b.GetTenantTableName(tenantID, parentTable),
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", truncateRebuildTableDDL, err)
	}
	return buf.String(), nil
}
