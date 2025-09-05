package partition

import (
	"bytes"
	"fmt"
)

const attachTenantPartitionDDL = `ALTER TABLE "{{.ParentTable}}" ATTACH PARTITION "{{.TenantTable}}" FOR VALUES IN ('{{.TenantID}}');`

type AttachTenantPartitionStatementData struct {
	ParentTable string
	TenantTable string
	TenantID    string
}

func (b *Builder) AttachTenantPartition(tenantID, parentTable string) (statement string, err error) {
	cleanID := NotAllowedCharacters.ReplaceAllString(tenantID, "")
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, attachTenantPartitionDDL, AttachTenantPartitionStatementData{
		ParentTable: parentTable,
		TenantTable: b.GetTenantTableName(tenantID, parentTable),
		TenantID:    cleanID,
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", attachTenantPartitionDDL, err)
	}
	return buf.String(), nil
}
