package main

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// UIConfig holds configuration for the ddb ui command.
// Loaded from ddb.ui.yaml if present.
type UIConfig struct {
	// DataDir is where BadgerDB stores data for the UI.
	DataDir string `yaml:"dataDir"`

	// Port is the HTTP port for the UI server.
	Port int `yaml:"port"`
}

// LoadUIConfig searches for ddb.ui.yaml starting from the current directory
// and walking up to the filesystem root. Returns empty config if not found.
func LoadUIConfig() UIConfig {
	var cfg UIConfig

	configPath := findUIConfigFile()
	if configPath == "" {
		return cfg
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg
	}

	_ = yaml.Unmarshal(data, &cfg)
	return cfg
}

// findUIConfigFile searches for ddb.ui.yaml walking up from current directory.
func findUIConfigFile() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		path := filepath.Join(dir, "ddb.ui.yaml")
		if _, err := os.Stat(path); err == nil {
			return path
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			return ""
		}
		dir = parent
	}
}
