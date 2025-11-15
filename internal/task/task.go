package task

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// EndpointCondition methods
func (c *EndpointCondition) Evaluate() (bool, error) {
	// TODO: Implement actual evaluation logic
	return false, nil
}

func (c *EndpointCondition) GetType() ConditionType {
	return c.Type
}

// EndpointAction methods
func (a *EndpointAction) Execute() error {
	return nil
}

func (a *EndpointAction) GetType() ActionType {
	return ActionTypeEndpoint
}

// UnmarshalYAML implements custom YAML unmarshaling for Task
func (t *Task) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var taskNode struct {
		When yaml.Node `yaml:"when"`
		Then yaml.Node `yaml:"then"`
	}

	if err := unmarshal(&taskNode); err != nil {
		return err
	}

	// Unmarshal Condition - first check the type field
	var bareCondition struct {
		Type ConditionType `yaml:"type"`
	}
	if err := taskNode.When.Decode(&bareCondition); err != nil {
		return fmt.Errorf("failed to decode condition type: %w", err)
	}

	switch bareCondition.Type {
	case ConditionTypeEndpoint:
		var condition EndpointCondition
		if err := taskNode.When.Decode(&condition); err != nil {
			return fmt.Errorf("failed to unmarshal condition: %w", err)
		}
		t.When = &condition
	default:
		return fmt.Errorf("unknown condition type: %s", bareCondition.Type)
	}

	// Unmarshal Action - first check the type field
	var bareAction struct {
		Type ActionType `yaml:"type"`
	}
	if err := taskNode.Then.Decode(&bareAction); err != nil {
		return fmt.Errorf("failed to decode action type: %w", err)
	}

	switch bareAction.Type {
	case ActionTypeEndpoint:
		var action EndpointAction
		if err := taskNode.Then.Decode(&action); err != nil {
			return fmt.Errorf("failed to unmarshal action: %w", err)
		}
		t.Then = &action
	default:
		return fmt.Errorf("unknown action type: %s", bareAction.Type)
	}

	return nil
}

// LoadConfig reads and parses a YAML configuration file
func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &config, nil
}
