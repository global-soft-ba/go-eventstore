package stdout

import "log"

type Adapter struct{}

func (a Adapter) Info(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (a Adapter) Warn(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (a Adapter) Error(err error) {
	log.Printf("%v", err)
}

func (a Adapter) Fatal(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}
