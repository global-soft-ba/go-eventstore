package noop

type Adapter struct{}

func (a Adapter) Info(format string, v ...interface{}) {
	return
}

func (a Adapter) Warn(format string, v ...interface{}) {
	return
}

func (a Adapter) Error(err error) {
	return
}

func (a Adapter) Fatal(format string, v ...interface{}) {
	return
}
