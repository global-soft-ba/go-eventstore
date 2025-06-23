package partition

func (b *Builder) GetAllTablesOfTenant(tenantID, parentTable string) (string, []interface{}, error) {
	return b.DoesTableExist(
		b.GetDatabaseSchema(),
		b.GetTenantTableName(tenantID, parentTable),
		b.GetTempTableName(tenantID, parentTable))
}
