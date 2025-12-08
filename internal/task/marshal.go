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

func (a *ActionConfigValueSum) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var actionNode struct {
		Type          ActionType `yaml:"type"`
		ConfigMapName string     `yaml:"config_map"`
		Key           string     `yaml:"key"`
		Sum           int        `yaml:"sum"`
		Members       []string   `yaml:"members"`
		OnChange      yaml.Node  `yaml:"onChange"`
	}

	if err := unmarshal(&actionNode); err != nil {
		return err
	}

	a.Type = actionNode.Type
	a.ConfigMapName = actionNode.ConfigMapName
	a.Key = actionNode.Key
	a.Sum = actionNode.Sum
	a.Members = actionNode.Members

	// Unmarshal OnChange if present
	if actionNode.OnChange.Kind != 0 {
		// Handle both list and single object formats
		var onChangeNode yaml.Node = actionNode.OnChange

		// If it's a sequence (list), take the first element
		if actionNode.OnChange.Kind == yaml.SequenceNode && len(actionNode.OnChange.Content) > 0 {
			onChangeNode = *actionNode.OnChange.Content[0]
		}

		var bareOnChange struct {
			Type OnChangeType `yaml:"type"`
		}
		if err := onChangeNode.Decode(&bareOnChange); err != nil {
			return fmt.Errorf("failed to decode onChange type: %w", err)
		}

		switch bareOnChange.Type {
		case OnChangeTypeDeploymentRestart:
			var onChange OnChangeDeploymentRestart
			if err := onChangeNode.Decode(&onChange); err != nil {
				return fmt.Errorf("failed to unmarshal onChange: %w", err)
			}
			a.OnChange = &onChange
		default:
			return fmt.Errorf("unknown onChange type: %s", bareOnChange.Type)
		}
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
	case ConditionTypeAlwaysTrue:
		var condition ConditionAlwaysTrue
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
	case ActionTypeEcho:
		var action ActionEcho
		if err := taskNode.Then.Decode(&action); err != nil {
			return fmt.Errorf("failed to unmarshal action: %w", err)
		}
		t.Then = &action
	case ActionTypeConfigValueSum:
		var action ActionConfigValueSum
		if err := taskNode.Then.Decode(&action); err != nil {
			return fmt.Errorf("failed to unmarshal action: %w", err)
		}
		t.Then = &action
	case ActionTypeK8sExecDeployment:
		var action ActionK8sExecDeployment
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
