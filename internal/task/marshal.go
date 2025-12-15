package task

import (
	"fmt"
	"os"
	"time"

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

func (c *ConditionPrometheusMetric) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ConditionPrometheusMetric
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*c = ConditionPrometheusMetric(aux)
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
	}

	if err := unmarshal(&actionNode); err != nil {
		return err
	}

	a.Type = actionNode.Type
	a.ConfigMapName = actionNode.ConfigMapName
	a.Key = actionNode.Key
	a.Sum = actionNode.Sum
	a.Members = actionNode.Members

	return nil
}

func (a *ActionK8sRestartDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ActionK8sRestartDeployment
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*a = ActionK8sRestartDeployment(aux)
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias ActionK8sWaitDeploymentRollout
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*a = ActionK8sWaitDeploymentRollout(aux)
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	if a.Timeout == 0 {
		a.Timeout = 5 * time.Minute
	}
	return nil
}

func (t *Task) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var taskNode struct {
		Name string      `yaml:"name"`
		When []yaml.Node `yaml:"when"`
		Then []yaml.Node `yaml:"then"`
	}

	if err := unmarshal(&taskNode); err != nil {
		return err
	}

	t.Name = taskNode.Name

	t.When = make([]Condition, 0, len(taskNode.When))
	for i, conditionNode := range taskNode.When {
		var bareCondition struct {
			Type ConditionType `yaml:"type"`
		}
		if err := conditionNode.Decode(&bareCondition); err != nil {
			return fmt.Errorf("failed to decode condition type at index %d: %w", i, err)
		}

		var condition Condition
		switch bareCondition.Type {
		case ConditionTypeEndpointSuccess:
			var c ConditionEndpointSuccess
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case ConditionTypeEndpointValue:
			var c ConditionEndpointValue
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case ConditionTypePrometheusMetric:
			var c ConditionPrometheusMetric
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case ConditionTypeAlwaysTrue:
			var c ConditionAlwaysTrue
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		default:
			return fmt.Errorf("unknown condition type at index %d: %s", i, bareCondition.Type)
		}
		t.When = append(t.When, condition)
	}

	t.Then = make([]Action, 0, len(taskNode.Then))
	for i, actionNode := range taskNode.Then {
		var bareAction struct {
			Type ActionType `yaml:"type"`
		}
		if err := actionNode.Decode(&bareAction); err != nil {
			return fmt.Errorf("failed to decode action type at index %d: %w", i, err)
		}

		var action Action
		switch bareAction.Type {
		case ActionTypeEndpoint:
			var a ActionEndpoint
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeEcho:
			var a ActionEcho
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeDelay:
			var a ActionDelay
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeConfigValueSum:
			var a ActionConfigValueSum
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeK8sExecDeployment:
			var a ActionK8sExecDeployment
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeK8sRestartDeployment:
			var a ActionK8sRestartDeployment
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case ActionTypeK8sWaitDeploymentRollout:
			var a ActionK8sWaitDeploymentRollout
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		default:
			return fmt.Errorf("unknown action type at index %d: %s", i, bareAction.Type)
		}
		t.Then = append(t.Then, action)
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
