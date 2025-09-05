package logger

var (
	logger Port
)

type Port interface {
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(err error)
	Fatal(format string, v ...interface{})
}

func SetLogger(l Port) {
	logger = l
}

func IsLoggerSet() bool {
	return logger != nil
}

func Info(format string, v ...interface{}) {
	if logger != nil {
		logger.Info(format, v...)
	}
}

func Warn(format string, v ...interface{}) {
	if logger != nil {
		logger.Warn(format, v...)
	}
}

func Error(err error) {
	if logger != nil {
		logger.Error(err)
	}
}

func Fatal(format string, v ...interface{}) {
	if logger != nil {
		logger.Fatal(format, v...)
	}
}
