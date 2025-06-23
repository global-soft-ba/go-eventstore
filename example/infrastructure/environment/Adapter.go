package environment

import (
	"example/core/port/environment"
	"fmt"
	"github.com/caarlos0/env/v11"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/joho/godotenv"
	"path/filepath"
	"runtime"
)

func InitEnvironment(envMode string) {
	if envMode == "" {
		envMode = environment.Local
	}
	if err := loadEnvironmentVariablesFromFile(envMode); err != nil {
		logger.Fatal("environment startup error while loading from file, error: %+v", err)
	}
	cfg := environment.EnvConfig{}
	if err := env.Parse(&cfg); err != nil {
		logger.Fatal("environment startup error while parsing variables, error: %+v", err)
	}
	environment.SetConfig(cfg)
}

func loadEnvironmentVariablesFromFile(environment string) error {
	_, b, _, _ := runtime.Caller(0)
	dir := filepath.Dir(b)
	configFile := fmt.Sprintf("../../config/%s.env", environment)
	configFilePath := filepath.Join(dir, configFile)
	if err := godotenv.Load(configFilePath); err != nil {
		return err
	}
	return nil
}
