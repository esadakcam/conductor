package utils

import (
	"fmt"
	"os"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/server"
	"github.com/esadakcam/conductor/internal/task"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Name          string              `yaml:"name"`
	EtcdEndpoints []string            `yaml:"db"`
	Server        server.ServerConfig `yaml:"server"`
	Tasks         []task.Task         `yaml:"tasks"`
}

// LoadConfig reads the configuration from the config file
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("LoadConfig: failed to read config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		logger.Errorf("LoadConfig: failed to parse config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if len(config.EtcdEndpoints) == 0 {
		// Default to localhost endpoints if not specified
		config.EtcdEndpoints = []string{"http://localhost:2379", "http://localhost:2479", "http://localhost:2579"}
	}

	if config.Name == "" {
		err := fmt.Errorf("name is required in config file")
		logger.Error("LoadConfig: name is required in config file")
		return nil, err
	}

	// Set default port if not specified
	if config.Server.Port == 0 {
		config.Server.Port = 8080
	}

	return &config, nil
}
