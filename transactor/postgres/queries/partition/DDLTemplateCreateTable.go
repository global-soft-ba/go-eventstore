package partition

import (
	"bytes"
	"fmt"
)

const createTableIfNotExistsDDL = `
	CREATE TABLE IF NOT EXISTS "{{.ChildTable}}" (LIKE "{{.ParentTable}}" INCLUDING ALL);
`

type CreateTableStatementData struct {
	ParentTable string
	ChildTable  string
}

func (b *Builder) CreateNewTenantTable(tenantID, parentTable string) (string, error) {
	cleanID := NotAllowedCharacters.ReplaceAllString(tenantID, "")
	return b.createTable(parentTable, b.GetTenantTableName(cleanID, parentTable))
}

func (b *Builder) CreateNewRebuildTable(tenantID, parentTable string) (string, error) {
	cleanID := NotAllowedCharacters.ReplaceAllString(tenantID, "")
	return b.createTable(b.GetTempTableName(cleanID, parentTable), b.GetTenantTableName(cleanID, parentTable))
}

func (b *Builder) createTable(parentTable, childTable string) (statement string, err error) {
	var buf bytes.Buffer
	if err = b.Templates.ExecuteTemplate(&buf, createTableIfNotExistsDDL, CreateTableStatementData{
		ParentTable: parentTable,
		ChildTable:  childTable,
	}); err != nil {
		return "", fmt.Errorf("error executing template %s: %w", createTableIfNotExistsDDL, err)
	}
	return buf.String(), nil
}
