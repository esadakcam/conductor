package executor

import (
	"fmt"
	"time"

	"github.com/esadakcam/conductor/internal/task"
	"gopkg.in/yaml.v3"
)

// Factory creates condition and action instances from YAML config
type Factory struct{}

func NewFactory() *Factory {
	return &Factory{}
}

// DecodeCondition decodes a condition from a YAML node
func (f *Factory) DecodeCondition(node *yaml.Node, index int) (task.Condition, error) {
	var bareCondition struct {
		Type task.ConditionType `yaml:"type"`
	}
	if err := node.Decode(&bareCondition); err != nil {
		return nil, fmt.Errorf("failed to decode condition type at index %d: %w", index, err)
	}

	var condition task.Condition
	switch bareCondition.Type {
	case task.ConditionTypeEndpointSuccess:
		c := &ConditionEndpointSuccess{}
		if err := decodeWithDefaults(node, &c.ConditionEndpointSuccess); err != nil {
			return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
		}
		if c.Status == 0 {
			c.Status = 200
		}
		condition = c
	case task.ConditionTypeEndpointValue:
		c := &ConditionEndpointValue{}
		if err := decodeWithDefaults(node, &c.ConditionEndpointValue); err != nil {
			return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
		}
		if c.Operator == "" {
			c.Operator = "eq"
		}
		condition = c
	case task.ConditionTypePrometheusMetric:
		c := &ConditionPrometheusMetric{}
		if err := decodeWithDefaults(node, &c.ConditionPrometheusMetric); err != nil {
			return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
		}
		if c.Operator == "" {
			c.Operator = "eq"
		}
		condition = c
	case task.ConditionTypeAlwaysTrue:
		c := &ConditionAlwaysTrue{}
		if err := node.Decode(&c.ConditionAlwaysTrue); err != nil {
			return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
		}
		condition = c
	case task.ConditionTypeK8sDeploymentReady:
		c := &ConditionK8sDeploymentReady{}
		if err := decodeWithDefaults(node, &c.ConditionK8sDeploymentReady); err != nil {
			return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
		}
		if c.Namespace == "" {
			c.Namespace = "default"
		}
		condition = c
	default:
		return nil, fmt.Errorf("unknown condition type at index %d: %s", index, bareCondition.Type)
	}

	return condition, nil
}

// DecodeAction decodes an action from a YAML node
func (f *Factory) DecodeAction(node *yaml.Node, index int) (task.Action, error) {
	var bareAction struct {
		Type task.ActionType `yaml:"type"`
	}
	if err := node.Decode(&bareAction); err != nil {
		return nil, fmt.Errorf("failed to decode action type at index %d: %w", index, err)
	}

	var action task.Action
	switch bareAction.Type {
	case task.ActionTypeEndpoint:
		a := &ActionEndpoint{}
		if err := decodeWithDefaults(node, &a.ActionEndpoint); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Method == "" {
			a.Method = "GET"
		}
		action = a
	case task.ActionTypeEcho:
		a := &ActionEcho{}
		if err := node.Decode(&a.ActionEcho); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		action = a
	case task.ActionTypeDelay:
		a := &ActionDelay{}
		if err := node.Decode(&a.ActionDelay); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		action = a
	case task.ActionTypeConfigValueSum:
		a := &ActionConfigValueSum{}
		if err := decodeConfigValueSum(node, &a.ActionConfigValueSum); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		action = a
	case task.ActionTypeK8sExecDeployment:
		a := &ActionK8sExecDeployment{}
		if err := decodeWithDefaults(node, &a.ActionK8sExecDeployment); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		action = a
	case task.ActionTypeK8sRestartDeployment:
		a := &ActionK8sRestartDeployment{}
		if err := decodeWithDefaults(node, &a.ActionK8sRestartDeployment); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		action = a
	case task.ActionTypeK8sWaitDeploymentRollout:
		a := &ActionK8sWaitDeploymentRollout{}
		if err := decodeWithDefaults(node, &a.ActionK8sWaitDeploymentRollout); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		if a.Timeout == 0 {
			a.Timeout = 5 * time.Minute
		}
		action = a
	case task.ActionTypeK8sUpdateConfigMap:
		a := &ActionK8sUpdateConfigMap{}
		if err := decodeWithDefaults(node, &a.ActionK8sUpdateConfigMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		action = a
	case task.ActionTypeK8sScaleDeployment:
		a := &ActionK8sScaleDeployment{}
		if err := decodeWithDefaults(node, &a.ActionK8sScaleDeployment); err != nil {
			return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
		}
		if a.Namespace == "" {
			a.Namespace = "default"
		}
		action = a
	default:
		return nil, fmt.Errorf("unknown action type at index %d: %s", index, bareAction.Type)
	}

	return action, nil
}

// UnmarshalTask unmarshals a task from a YAML node
func (f *Factory) UnmarshalTask(node *yaml.Node, index int) (*task.Task, error) {
	var taskNode struct {
		Name string      `yaml:"name"`
		When []yaml.Node `yaml:"when"`
		Then []yaml.Node `yaml:"then"`
	}

	if err := node.Decode(&taskNode); err != nil {
		return nil, fmt.Errorf("failed to decode task at index %d: %w", index, err)
	}

	t := &task.Task{
		Name: taskNode.Name,
		When: make([]task.Condition, 0, len(taskNode.When)),
		Then: make([]task.Action, 0, len(taskNode.Then)),
	}

	for i, conditionNode := range taskNode.When {
		condition, err := f.DecodeCondition(&conditionNode, i)
		if err != nil {
			return nil, err
		}
		t.When = append(t.When, condition)
	}

	for i, actionNode := range taskNode.Then {
		action, err := f.DecodeAction(&actionNode, i)
		if err != nil {
			return nil, err
		}
		t.Then = append(t.Then, action)
	}

	return t, nil
}

func decodeWithDefaults(node *yaml.Node, target interface{}) error {
	return node.Decode(target)
}

func decodeConfigValueSum(node *yaml.Node, a *task.ActionConfigValueSum) error {
	var actionNode struct {
		Type          task.ActionType `yaml:"type"`
		ConfigMapName string          `yaml:"config_map"`
		Namespace     string          `yaml:"namespace,omitempty"`
		Key           string          `yaml:"key"`
		Sum           int             `yaml:"sum"`
		Members       []string        `yaml:"members"`
	}

	if err := node.Decode(&actionNode); err != nil {
		return err
	}

	a.Type = actionNode.Type
	a.ConfigMapName = actionNode.ConfigMapName
	a.Namespace = actionNode.Namespace
	a.Key = actionNode.Key
	a.Sum = actionNode.Sum
	a.Members = actionNode.Members

	return nil
}
