package config

import "os"

type Config struct {
	ListnrAddr string
	TargetAddr string
}

func Load() *Config {

	listenAddr := os.Getenv("PGPROXY_LISTEN")
	if listenAddr == "" {
		listenAddr = ":5433" // Default proxy port
	}

	targetAddr := os.Getenv("PGPROXY_TARGET")
	if targetAddr == "" {
		targetAddr = "127.0.0.1:5432" // Default Postgres port
	}

	return &Config{
		ListnrAddr: listenAddr,
		TargetAddr: targetAddr,
	}
}
