// Package config handles loading and parsing of conductor configuration files.
package config

import (
	"fmt"
	"os"

	"github.com/esadakcam/conductor/internal/executor"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"gopkg.in/yaml.v3"
)

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port int `yaml:"port"`
}

// DistributedConfig holds configuration for distributed mode
type DistributedConfig struct {
	Name          string               `yaml:"name"`
	EtcdEndpoints []string             `yaml:"db"`
	Server        ServerConfig         `yaml:"server"`
	Tasks         []task.TaskInterface `yaml:"tasks"`
}

// CentralizedConfig holds configuration for centralized mode
type CentralizedConfig struct {
	Tasks               []task.TaskInterface `yaml:"tasks"`
	KubeconfigLocations []string             `yaml:"kubeconfig_locations"`
}

// distributedConfigRaw is used for the first pass of unmarshalling
type distributedConfigRaw struct {
	Name          string       `yaml:"name"`
	EtcdEndpoints []string     `yaml:"db"`
	Server        ServerConfig `yaml:"server"`
	Tasks         []yaml.Node  `yaml:"tasks"`
}

// centralizedConfigRaw is used for the first pass of unmarshalling
type centralizedConfigRaw struct {
	Tasks               []yaml.Node `yaml:"tasks"`
	KubeconfigLocations []string    `yaml:"kubeconfig_locations"`
}

// LoadDistributed loads and parses a distributed mode configuration file
func LoadDistributed(configPath string) (*DistributedConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("LoadDistributed: failed to read config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig distributedConfigRaw
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		logger.Errorf("LoadDistributed: failed to parse config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config := &DistributedConfig{
		Name:          rawConfig.Name,
		EtcdEndpoints: rawConfig.EtcdEndpoints,
		Server:        rawConfig.Server,
	}

	// Unmarshal tasks using the unified factory
	factory := executor.NewFactory()
	config.Tasks = make([]task.TaskInterface, 0, len(rawConfig.Tasks))
	for i, taskNode := range rawConfig.Tasks {
		t, err := factory.UnmarshalTask(&taskNode, i)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal task %d: %w", i, err)
		}
		config.Tasks = append(config.Tasks, t)
	}

	// Apply defaults
	if len(config.EtcdEndpoints) == 0 {
		config.EtcdEndpoints = []string{"http://localhost:2379", "http://localhost:2479", "http://localhost:2579"}
	}

	if config.Name == "" {
		return nil, fmt.Errorf("name is required in config file")
	}

	if config.Server.Port == 0 {
		config.Server.Port = 8080
	}

	return config, nil
}

// LoadCentralized loads and parses a centralized mode configuration file
func LoadCentralized(configPath string) (*CentralizedConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("LoadCentralized: failed to read config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig centralizedConfigRaw
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		logger.Errorf("LoadCentralized: failed to parse config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config := &CentralizedConfig{
		KubeconfigLocations: rawConfig.KubeconfigLocations,
	}

	// Unmarshal tasks using the unified factory
	factory := executor.NewFactory()
	config.Tasks = make([]task.TaskInterface, 0, len(rawConfig.Tasks))
	for i, taskNode := range rawConfig.Tasks {
		t, err := factory.UnmarshalTask(&taskNode, i)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal task %d: %w", i, err)
		}
		config.Tasks = append(config.Tasks, t)
	}

	return config, nil
}
