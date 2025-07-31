package partition

import (
	"bytes"
	"fmt"
)

const getPartitionsOfTenantDDL = `SELECT inhrelid::regclass AS partition FROM pg_catalog.pg_inherits WHERE inhparent = '{{.ParentTable}}'::regclass AND CAST(inhrelid::regclass AS VARCHAR) LIKE '%{{.TenantID}}%';`

type GetPartitionsOfTableStatementData struct {
	ParentTable string
	TenantID    string
}

func (b *Builder) GetAllPartitionsOfTenant(tenantID, parentTable string) (statement string, err error) {
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, getPartitionsOfTenantDDL, GetPartitionsOfTableStatementData{
		ParentTable: parentTable,
		TenantID:    tenantID,
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", getPartitionsOfTenantDDL, err)
	}
	return buf.String(), nil
}
