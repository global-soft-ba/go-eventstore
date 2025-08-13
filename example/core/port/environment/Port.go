package environment

import "time"

var (
	env EnvConfig
)

const (
	Local = "local"
	Test  = "test"
)

type EnvConfig struct {

	// Application
	HttpServerPort int `env:"HTTP_SERVER_PORT" envDefault:"8080"`

	// Postgres
	DatabaseDialect                string `env:"DATABASE_DIALECT" envDefault:"postgres"`
	DatabaseHost                   string `env:"DATABASE_HOST" envDefault:"localhost"`
	DatabasePort                   string `env:"DATABASE_PORT" envDefault:"5432"`
	DatabaseUser                   string `env:"DATABASE_USER" envDefault:"example"`
	DatabaseSchema                 string `env:"DATABASE_SCHEMA" envDefault:"example_db"`
	DatabasePassword               string `env:"DATABASE_PASSWORD" envDefault:"example"`
	DatabaseMaxNumberOfConnections int    `env:"DATABASE_MAX_NUMBER_OF_CONNECTIONS" envDefault:"10"`

	// Projection
	ProjectionChunkSize      int           `env:"PROJECTION_CHUNK_SIZE" envDefault:"1000"`
	ProjectionRebuildTimeout time.Duration `env:"PROJECTION_REBUILD_TIMEOUT" envDefault:"5h"`
	ProjectionTimeout        time.Duration `env:"PROJECTION_TIMEOUT" envDefault:"5m"`
}

func SetConfig(envConfig EnvConfig) {
	env = envConfig
}

func Config() EnvConfig {
	return env
}
