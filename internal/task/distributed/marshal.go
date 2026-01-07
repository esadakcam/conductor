package distributed

import (
	"fmt"
	"os"
	"time"

	"github.com/esadakcam/conductor/internal/task"
	"gopkg.in/yaml.v3"
)

func (c *ConditionEndpointSuccess) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ConditionEndpointSuccess
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.ConditionEndpointSuccess = aux
	if c.Status == 0 {
		c.Status = 200
	}
	return nil
}

func (c *ConditionEndpointValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ConditionEndpointValue
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.ConditionEndpointValue = aux
	if c.Operator == "" {
		c.Operator = "eq"
	}
	return nil
}

func (c *ConditionPrometheusMetric) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ConditionPrometheusMetric
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.ConditionPrometheusMetric = aux
	if c.Operator == "" {
		c.Operator = "eq"
	}
	return nil
}

func (c *ConditionK8sDeploymentReady) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ConditionK8sDeploymentReady
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.ConditionK8sDeploymentReady = aux
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	return nil
}

func (c *ConditionAlwaysTrue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ConditionAlwaysTrue
	if err := unmarshal(&aux); err != nil {
		return err
	}
	c.ConditionAlwaysTrue = aux
	return nil
}

func (a *ActionEndpoint) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionEndpoint
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionEndpoint = aux
	if a.Method == "" {
		a.Method = "GET"
	}
	return nil
}

func (a *ActionConfigValueSum) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var actionNode struct {
		Type          task.ActionType `yaml:"type"`
		ConfigMapName string          `yaml:"config_map"`
		Key           string          `yaml:"key"`
		Sum           int             `yaml:"sum"`
		Members       []string        `yaml:"members"`
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

func (a *ActionEcho) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionEcho
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionEcho = aux
	return nil
}

func (a *ActionDelay) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionDelay
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionDelay = aux
	return nil
}

func (a *ActionK8sExecDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionK8sExecDeployment
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionK8sExecDeployment = aux
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func (a *ActionK8sRestartDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionK8sRestartDeployment
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionK8sRestartDeployment = aux
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionK8sWaitDeploymentRollout
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionK8sWaitDeploymentRollout = aux
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	if a.Timeout == 0 {
		a.Timeout = 5 * time.Minute
	}
	return nil
}

func (a *ActionK8sUpdateConfigMap) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionK8sUpdateConfigMap
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionK8sUpdateConfigMap = aux
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func (a *ActionK8sScaleDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var aux task.ActionK8sScaleDeployment
	if err := unmarshal(&aux); err != nil {
		return err
	}
	a.ActionK8sScaleDeployment = aux
	if a.Namespace == "" {
		a.Namespace = "default"
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

	t.When = make([]task.Condition, 0, len(taskNode.When))
	for i, conditionNode := range taskNode.When {
		var bareCondition struct {
			Type task.ConditionType `yaml:"type"`
		}
		if err := conditionNode.Decode(&bareCondition); err != nil {
			return fmt.Errorf("failed to decode condition type at index %d: %w", i, err)
		}

		var condition task.Condition
		switch bareCondition.Type {
		case task.ConditionTypeEndpointSuccess:
			var c ConditionEndpointSuccess
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case task.ConditionTypeEndpointValue:
			var c ConditionEndpointValue
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case task.ConditionTypePrometheusMetric:
			var c ConditionPrometheusMetric
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case task.ConditionTypeAlwaysTrue:
			var c ConditionAlwaysTrue
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		case task.ConditionTypeK8sDeploymentReady:
			var c ConditionK8sDeploymentReady
			if err := conditionNode.Decode(&c); err != nil {
				return fmt.Errorf("failed to unmarshal condition at index %d: %w", i, err)
			}
			condition = &c
		default:
			return fmt.Errorf("unknown condition type at index %d: %s", i, bareCondition.Type)
		}
		t.When = append(t.When, condition)
	}

	t.Then = make([]task.Action, 0, len(taskNode.Then))
	for i, actionNode := range taskNode.Then {
		var bareAction struct {
			Type task.ActionType `yaml:"type"`
		}
		if err := actionNode.Decode(&bareAction); err != nil {
			return fmt.Errorf("failed to decode action type at index %d: %w", i, err)
		}

		var action task.Action
		switch bareAction.Type {
		case task.ActionTypeEndpoint:
			var a ActionEndpoint
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeEcho:
			var a ActionEcho
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeDelay:
			var a ActionDelay
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeConfigValueSum:
			var a ActionConfigValueSum
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeK8sExecDeployment:
			var a ActionK8sExecDeployment
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeK8sRestartDeployment:
			var a ActionK8sRestartDeployment
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeK8sWaitDeploymentRollout:
			var a ActionK8sWaitDeploymentRollout
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeK8sUpdateConfigMap:
			var a ActionK8sUpdateConfigMap
			if err := actionNode.Decode(&a); err != nil {
				return fmt.Errorf("failed to unmarshal action at index %d: %w", i, err)
			}
			action = &a
		case task.ActionTypeK8sScaleDeployment:
			var a ActionK8sScaleDeployment
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

func LoadConfig(filePath string) (*task.Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config task.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &config, nil
}
