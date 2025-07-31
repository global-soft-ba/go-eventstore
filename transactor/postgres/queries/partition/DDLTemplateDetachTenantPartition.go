package partition

import (
	"bytes"
	"fmt"
)

const detachTenantPartitionDDL = `ALTER TABLE "{{.ParentTable}}" DETACH PARTITION "{{.TenantTable}}";`

type DetachTenantPartitionStatementData struct {
	ParentTable string
	TenantTable string
}

func (b *Builder) DetachTempTablePartition(tenantID, parentTable string) (statement string, err error) {
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, detachTenantPartitionDDL, DetachTenantPartitionStatementData{
		ParentTable: parentTable,
		TenantTable: b.GetTempTableName(tenantID, parentTable),
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", detachTenantPartitionDDL, err)
	}
	return buf.String(), nil
}
