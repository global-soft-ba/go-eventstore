package partition

import (
	"bytes"
	"fmt"
)

const dropRebuildTableDDL = `DROP TABLE IF EXISTS "{{.RebuildTable}}";`

type DropRebuildTableStatementData struct {
	RebuildTable string
}

func (b *Builder) DropRebuildTable(tenantID, parentTable string) (statement string, err error) {
	cleanID := NotAllowedCharacters.ReplaceAllString(tenantID, "")
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, dropRebuildTableDDL, DropRebuildTableStatementData{
		RebuildTable: b.GetTempTableName(cleanID, parentTable),
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", dropRebuildTableDDL, err)
	}
	return buf.String(), nil
}
