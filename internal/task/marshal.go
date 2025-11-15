package task

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func (c *ConditionEndpointSuccess) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ConditionEndpointSuccess
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*c = ConditionEndpointSuccess(aux)
	if c.Status == 0 {
		c.Status = 200
	}
	return nil
}

func (c *ConditionEndpointValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ConditionEndpointValue
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*c = ConditionEndpointValue(aux)
	if c.Operator == "" {
		c.Operator = "eq"
	}
	return nil
}

func (a *ActionEndpoint) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ActionEndpoint
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*a = ActionEndpoint(aux)
	if a.Method == "" {
		a.Method = "GET"
	}
	return nil
}

func (t *Task) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var taskNode struct {
		When yaml.Node `yaml:"when"`
		Then yaml.Node `yaml:"then"`
	}

	if err := unmarshal(&taskNode); err != nil {
		return err
	}

	var bareCondition struct {
		Type ConditionType `yaml:"type"`
	}
	if err := taskNode.When.Decode(&bareCondition); err != nil {
		return fmt.Errorf("failed to decode condition type: %w", err)
	}

	switch bareCondition.Type {
	case ConditionTypeEndpointSuccess:
		var condition ConditionEndpointSuccess
		if err := taskNode.When.Decode(&condition); err != nil {
			return fmt.Errorf("failed to unmarshal condition: %w", err)
		}
		t.When = &condition
	case ConditionTypeEndpointValue:
		var condition ConditionEndpointValue
		if err := taskNode.When.Decode(&condition); err != nil {
			return fmt.Errorf("failed to unmarshal condition: %w", err)
		}
		t.When = &condition
	default:
		return fmt.Errorf("unknown condition type: %s", bareCondition.Type)
	}

	var bareAction struct {
		Type ActionType `yaml:"type"`
	}
	if err := taskNode.Then.Decode(&bareAction); err != nil {
		return fmt.Errorf("failed to decode action type: %w", err)
	}

	switch bareAction.Type {
	case ActionTypeEndpoint:
		var action ActionEndpoint
		if err := taskNode.Then.Decode(&action); err != nil {
			return fmt.Errorf("failed to unmarshal action: %w", err)
		}
		t.Then = &action
	default:
		return fmt.Errorf("unknown action type: %s", bareAction.Type)
	}

	return nil
}

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
