package tables

const AdvisoryLockAliasName = "Locked" //same name as in struct AdvisoryLock

type AdvisoryLock struct {
	Locked bool
}

func (a AdvisoryLock) LockAlias() string {
	return AdvisoryLockAliasName
}
