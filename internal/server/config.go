package server

import (
	"os"
	"strconv"
)

// Config holds server configuration from environment variables.
type Config struct {
	Port        string
	DatabaseURL string
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	return Config{
		Port:        getEnv("OJS_PORT", "8080"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://localhost:5432/ojs?sslmode=disable"),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return defaultVal
}
