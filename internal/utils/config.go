package utils

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// GetEtcdEndpoints reads etcd endpoints from the config file
func GetEtcdEndpoints(configPath string) ([]string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config struct {
		DB []string `yaml:"db"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if len(config.DB) == 0 {
		// Default to localhost endpoints if not specified
		return []string{"http://localhost:2379", "http://localhost:2479", "http://localhost:2579"}, nil
	}

	return config.DB, nil
}

// GetName reads the name field from the config file
func GetName(configPath string) (string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("failed to read config file: %w", err)
	}

	var config struct {
		Name string `yaml:"name"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("failed to parse config: %w", err)
	}

	if config.Name == "" {
		return "", fmt.Errorf("name is required in config file")
	}

	return config.Name, nil
}
