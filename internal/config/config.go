package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Server struct {
		Port      int `json:"port"`
		AdminPort int `json:"admin_port"`
	} `json:"server"`
	Exchanges map[string]ExchangeConfig `json:"exchanges"`
}

type ExchangeConfig struct {
	Symbols []string `json:"symbols"`
	Depth   int      `json:"depth"`
}

func MustLoad(path string) *Config {
	cfg, err := load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

func load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode %q: %w", path, err)
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	if cfg.Server.AdminPort == 0 {
		cfg.Server.AdminPort = 9090
	}
	for name, ex := range cfg.Exchanges {
		if ex.Depth == 0 {
			ex.Depth = 10
		}
		cfg.Exchanges[name] = ex
	}
	return &cfg, nil
}
