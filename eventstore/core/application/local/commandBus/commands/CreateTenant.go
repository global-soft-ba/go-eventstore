package commands

func CmdCreateTenant(tenantID string) CreateTenant {
	return CreateTenant{
		id: tenantID,
	}
}

type CreateTenant struct {
	id string
}

func (e CreateTenant) Name() string {
	return "cmd.tenant.create"
}

func (e CreateTenant) ID() string {
	return e.id
}
