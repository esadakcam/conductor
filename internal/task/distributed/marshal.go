package distributed

import (
	"fmt"
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
		condition, err := decodeCondition(conditionNode, i)
		if err != nil {
			return err
		}
		t.When = append(t.When, condition)
	}

	t.Then = make([]task.Action, 0, len(taskNode.Then))
	for i, actionNode := range taskNode.Then {
		action, err := decodeAction(actionNode, i)
		if err != nil {
			return err
		}
		t.Then = append(t.Then, action)
	}

	return nil
}

func decodeCondition(node yaml.Node, index int) (task.Condition, error) {
	var bareCondition struct {
		Type task.ConditionType `yaml:"type"`
	}
	if err := node.Decode(&bareCondition); err != nil {
		return nil, fmt.Errorf("failed to decode condition type at index %d: %w", index, err)
	}

	switch bareCondition.Type {
	case task.ConditionTypeEndpointSuccess:
		return decodeConditionNode(node, index, &ConditionEndpointSuccess{})
	case task.ConditionTypeEndpointValue:
		return decodeConditionNode(node, index, &ConditionEndpointValue{})
	case task.ConditionTypePrometheusMetric:
		return decodeConditionNode(node, index, &ConditionPrometheusMetric{})
	case task.ConditionTypeAlwaysTrue:
		return decodeConditionNode(node, index, &ConditionAlwaysTrue{})
	case task.ConditionTypeK8sDeploymentReady:
		return decodeConditionNode(node, index, &ConditionK8sDeploymentReady{})
	default:
		return nil, fmt.Errorf("unknown condition type at index %d: %s", index, bareCondition.Type)
	}
}

func decodeConditionNode(node yaml.Node, index int, condition task.Condition) (task.Condition, error) {
	if err := node.Decode(condition); err != nil {
		return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
	}
	return condition, nil
}

func decodeAction(node yaml.Node, index int) (task.Action, error) {
	var bareAction struct {
		Type task.ActionType `yaml:"type"`
	}
	if err := node.Decode(&bareAction); err != nil {
		return nil, fmt.Errorf("failed to decode action type at index %d: %w", index, err)
	}

	switch bareAction.Type {
	case task.ActionTypeEndpoint:
		return decodeActionNode(node, index, &ActionEndpoint{})
	case task.ActionTypeEcho:
		return decodeActionNode(node, index, &ActionEcho{})
	case task.ActionTypeDelay:
		return decodeActionNode(node, index, &ActionDelay{})
	case task.ActionTypeConfigValueSum:
		return decodeActionNode(node, index, &ActionConfigValueSum{})
	case task.ActionTypeK8sExecDeployment:
		return decodeActionNode(node, index, &ActionK8sExecDeployment{})
	case task.ActionTypeK8sRestartDeployment:
		return decodeActionNode(node, index, &ActionK8sRestartDeployment{})
	case task.ActionTypeK8sWaitDeploymentRollout:
		return decodeActionNode(node, index, &ActionK8sWaitDeploymentRollout{})
	case task.ActionTypeK8sUpdateConfigMap:
		return decodeActionNode(node, index, &ActionK8sUpdateConfigMap{})
	case task.ActionTypeK8sScaleDeployment:
		return decodeActionNode(node, index, &ActionK8sScaleDeployment{})
	default:
		return nil, fmt.Errorf("unknown action type at index %d: %s", index, bareAction.Type)
	}
}

func decodeActionNode(node yaml.Node, index int, action task.Action) (task.Action, error) {
	if err := node.Decode(action); err != nil {
		return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
	}
	return action, nil
}
