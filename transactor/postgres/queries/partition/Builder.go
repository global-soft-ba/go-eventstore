package partition

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries"
	"regexp"
	"text/template"
)

const (
	// rebuildSuffix is used to rename tables during rebuild process
	rebuildSuffix = "tmp"
)

var NotAllowedCharacters = regexp.MustCompile(`[^a-zA-Z0-9\-_.]`)

type Builder struct {
	queries.SqlBuilder
	Templates *template.Template
}

func NewSqlBuilder(dataBaseSchema string, placeHolder sq.PlaceholderFormat) (*Builder, error) {
	tmpls, err := parseAllDDLTemplates()
	if err != nil {
		return nil, err
	}
	return &Builder{
		SqlBuilder: queries.NewSqlBuilder(dataBaseSchema, placeHolder),
		Templates:  tmpls,
	}, nil
}

func parseAllDDLTemplates() (*template.Template, error) {
	tmpls := template.New("")
	tmpls, err := tmpls.New(attachTenantPartitionDDL).Parse(attachTenantPartitionDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", attachTenantPartitionDDL, err)
	}
	tmpls, err = tmpls.New(createTableIfNotExistsDDL).Parse(createTableIfNotExistsDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", createTableIfNotExistsDDL, err)
	}
	tmpls, err = tmpls.New(detachTenantPartitionDDL).Parse(detachTenantPartitionDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", detachTenantPartitionDDL, err)
	}
	tmpls, err = tmpls.New(dropRebuildTableDDL).Parse(dropRebuildTableDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", dropRebuildTableDDL, err)
	}
	tmpls, err = tmpls.New(getPartitionsOfTenantDDL).Parse(getPartitionsOfTenantDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", getPartitionsOfTenantDDL, err)
	}
	tmpls, err = tmpls.New(renameTenantTableToRebuildTableDDL).Parse(renameTenantTableToRebuildTableDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", renameTenantTableToRebuildTableDDL, err)
	}
	tmpls, err = tmpls.New(truncateRebuildTableDDL).Parse(truncateRebuildTableDDL)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", truncateRebuildTableDDL, err)
	}
	return tmpls, nil
}

func (b *Builder) GetEscapedTenantTableName(tenantID string, tableName string) string {
	return b.WithEscapedName(b.GetTenantTableName(tenantID, tableName))
}

func (b *Builder) GetTenantTableName(tenantID string, tableName string) string {
	return fmt.Sprintf("%s_%s", tableName, tenantID)
}

func (b *Builder) GetTempTableName(tenantID string, tableName string) string {
	return fmt.Sprintf("%s_%s_%s", tableName, tenantID, rebuildSuffix)
}

// -----------------------------------------------------------------------------------------------------------------------
// --------------------------------------------TEST SUPPORT------------------------------------------------------------
// -----------------------------------------------------------------------------------------------------------------------

func (b *Builder) DropTenant(tenantID, parentTableName string) (string, []interface{}, error) {
	return sq.Expr(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", b.GetEscapedTenantTableName(tenantID, parentTableName))).ToSql()
}
