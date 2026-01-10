package utils

import (
	"fmt"
	"os"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/server"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/task/centralized"
	"github.com/esadakcam/conductor/internal/task/distributed"
	"gopkg.in/yaml.v3"
)

type DistributedConfig struct {
	Name          string               `yaml:"name"`
	EtcdEndpoints []string             `yaml:"db"`
	Server        server.ServerConfig  `yaml:"server"`
	Tasks         []task.TaskInterface `yaml:"tasks"`
}

type CentralizedConfig struct {
	Tasks               []task.TaskInterface `yaml:"tasks"`
	KubeconfigLocations []string             `yaml:"kubeconfig_locations"`
}

// distributedConfigRaw is used for the first pass of unmarshalling
type distributedConfigRaw struct {
	Name          string              `yaml:"name"`
	EtcdEndpoints []string            `yaml:"db"`
	Server        server.ServerConfig `yaml:"server"`
	Tasks         []yaml.Node         `yaml:"tasks"`
}

// centralizedConfigRaw is used for the first pass of unmarshalling
type centralizedConfigRaw struct {
	Tasks               []yaml.Node `yaml:"tasks"`
	KubeconfigLocations []string    `yaml:"kubeconfig_locations"`
}

func LoadDistributedConfig(configPath string) (*DistributedConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("LoadConfig: failed to read config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig distributedConfigRaw
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		logger.Errorf("LoadConfig: failed to parse config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config := &DistributedConfig{
		Name:          rawConfig.Name,
		EtcdEndpoints: rawConfig.EtcdEndpoints,
		Server:        rawConfig.Server,
	}

	// Unmarshal tasks using the distributed factory
	factory := distributed.GetFactory()
	config.Tasks = make([]task.TaskInterface, 0, len(rawConfig.Tasks))
	for i, taskNode := range rawConfig.Tasks {
		t, err := task.UnmarshalTaskFromNode(&taskNode, i, factory)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal task %d: %w", i, err)
		}
		config.Tasks = append(config.Tasks, t)
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

	return config, nil
}

func LoadCentralizedConfig(configPath string) (*CentralizedConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("LoadCentralizedConfig: failed to read config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig centralizedConfigRaw
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		logger.Errorf("LoadCentralizedConfig: failed to parse config file %s: %v", configPath, err)
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config := &CentralizedConfig{
		KubeconfigLocations: rawConfig.KubeconfigLocations,
	}

	// Unmarshal tasks using the centralized factory
	factory := centralized.GetFactory()
	config.Tasks = make([]task.TaskInterface, 0, len(rawConfig.Tasks))
	for i, taskNode := range rawConfig.Tasks {
		t, err := task.UnmarshalTaskFromNode(&taskNode, i, factory)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal task %d: %w", i, err)
		}
		config.Tasks = append(config.Tasks, t)
	}

	return config, nil
}
