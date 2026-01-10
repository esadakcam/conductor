package task

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// TypeFactory creates condition and action instances for unmarshalling
type TypeFactory interface {
	NewConditionEndpointSuccess() Condition
	NewConditionEndpointValue() Condition
	NewConditionPrometheusMetric() Condition
	NewConditionAlwaysTrue() Condition
	NewConditionK8sDeploymentReady() Condition
	NewActionEndpoint() Action
	NewActionEcho() Action
	NewActionDelay() Action
	NewActionConfigValueSum() Action
	NewActionK8sExecDeployment() Action
	NewActionK8sRestartDeployment() Action
	NewActionK8sWaitDeploymentRollout() Action
	NewActionK8sUpdateConfigMap() Action
	NewActionK8sScaleDeployment() Action
}

// UnmarshalTaskFromNode unmarshals a task from a yaml.Node using the provided factory
func UnmarshalTaskFromNode(node *yaml.Node, index int, factory TypeFactory) (*Task, error) {
	var taskNode struct {
		Name string      `yaml:"name"`
		When []yaml.Node `yaml:"when"`
		Then []yaml.Node `yaml:"then"`
	}

	if err := node.Decode(&taskNode); err != nil {
		return nil, fmt.Errorf("failed to decode task at index %d: %w", index, err)
	}

	t := &Task{
		Name: taskNode.Name,
		When: make([]Condition, 0, len(taskNode.When)),
		Then: make([]Action, 0, len(taskNode.Then)),
	}

	for i, conditionNode := range taskNode.When {
		condition, err := DecodeCondition(&conditionNode, i, factory)
		if err != nil {
			return nil, err
		}
		t.When = append(t.When, condition)
	}

	for i, actionNode := range taskNode.Then {
		action, err := DecodeAction(&actionNode, i, factory)
		if err != nil {
			return nil, err
		}
		t.Then = append(t.Then, action)
	}

	return t, nil
}

// DecodeCondition decodes a condition node using the factory
func DecodeCondition(node *yaml.Node, index int, factory TypeFactory) (Condition, error) {
	var bareCondition struct {
		Type ConditionType `yaml:"type"`
	}
	if err := node.Decode(&bareCondition); err != nil {
		return nil, fmt.Errorf("failed to decode condition type at index %d: %w", index, err)
	}

	var condition Condition
	switch bareCondition.Type {
	case ConditionTypeEndpointSuccess:
		condition = factory.NewConditionEndpointSuccess()
	case ConditionTypeEndpointValue:
		condition = factory.NewConditionEndpointValue()
	case ConditionTypePrometheusMetric:
		condition = factory.NewConditionPrometheusMetric()
	case ConditionTypeAlwaysTrue:
		condition = factory.NewConditionAlwaysTrue()
	case ConditionTypeK8sDeploymentReady:
		condition = factory.NewConditionK8sDeploymentReady()
	default:
		return nil, fmt.Errorf("unknown condition type at index %d: %s", index, bareCondition.Type)
	}

	if err := node.Decode(condition); err != nil {
		return nil, fmt.Errorf("failed to unmarshal condition at index %d: %w", index, err)
	}
	return condition, nil
}

// DecodeAction decodes an action node using the factory
func DecodeAction(node *yaml.Node, index int, factory TypeFactory) (Action, error) {
	var bareAction struct {
		Type ActionType `yaml:"type"`
	}
	if err := node.Decode(&bareAction); err != nil {
		return nil, fmt.Errorf("failed to decode action type at index %d: %w", index, err)
	}

	var action Action
	switch bareAction.Type {
	case ActionTypeEndpoint:
		action = factory.NewActionEndpoint()
	case ActionTypeEcho:
		action = factory.NewActionEcho()
	case ActionTypeDelay:
		action = factory.NewActionDelay()
	case ActionTypeConfigValueSum:
		action = factory.NewActionConfigValueSum()
	case ActionTypeK8sExecDeployment:
		action = factory.NewActionK8sExecDeployment()
	case ActionTypeK8sRestartDeployment:
		action = factory.NewActionK8sRestartDeployment()
	case ActionTypeK8sWaitDeploymentRollout:
		action = factory.NewActionK8sWaitDeploymentRollout()
	case ActionTypeK8sUpdateConfigMap:
		action = factory.NewActionK8sUpdateConfigMap()
	case ActionTypeK8sScaleDeployment:
		action = factory.NewActionK8sScaleDeployment()
	default:
		return nil, fmt.Errorf("unknown action type at index %d: %s", index, bareAction.Type)
	}

	if err := node.Decode(action); err != nil {
		return nil, fmt.Errorf("failed to unmarshal action at index %d: %w", index, err)
	}
	return action, nil
}

// UnmarshalYAML helper implementations for condition types

func UnmarshalConditionEndpointSuccess(unmarshal func(interface{}) error, c *ConditionEndpointSuccessData) error {
	type Alias ConditionEndpointSuccessData
	aux := (*Alias)(c)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if c.Status == 0 {
		c.Status = 200
	}
	return nil
}

func UnmarshalConditionEndpointValue(unmarshal func(interface{}) error, c *ConditionEndpointValueData) error {
	type Alias ConditionEndpointValueData
	aux := (*Alias)(c)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if c.Operator == "" {
		c.Operator = "eq"
	}
	return nil
}

func UnmarshalConditionPrometheusMetric(unmarshal func(interface{}) error, c *ConditionPrometheusMetricData) error {
	type Alias ConditionPrometheusMetricData
	aux := (*Alias)(c)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if c.Operator == "" {
		c.Operator = "eq"
	}
	return nil
}

func UnmarshalConditionK8sDeploymentReady(unmarshal func(interface{}) error, c *ConditionK8sDeploymentReadyData) error {
	type Alias ConditionK8sDeploymentReadyData
	aux := (*Alias)(c)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	return nil
}

func UnmarshalConditionAlwaysTrue(unmarshal func(interface{}) error, c *ConditionAlwaysTrueData) error {
	type Alias ConditionAlwaysTrueData
	aux := (*Alias)(c)
	return unmarshal(aux)
}

// UnmarshalYAML helper implementations for action types

func UnmarshalActionEndpoint(unmarshal func(interface{}) error, a *ActionEndpointData) error {
	type Alias ActionEndpointData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Method == "" {
		a.Method = "GET"
	}
	return nil
}

func UnmarshalActionConfigValueSum(unmarshal func(interface{}) error, a *ActionConfigValueSumData) error {
	var actionNode struct {
		Type          ActionType `yaml:"type"`
		ConfigMapName string     `yaml:"config_map"`
		Namespace     string     `yaml:"namespace,omitempty"`
		Key           string     `yaml:"key"`
		Sum           int        `yaml:"sum"`
		Members       []string   `yaml:"members"`
	}

	if err := unmarshal(&actionNode); err != nil {
		return err
	}
	if actionNode.Namespace == "" {
		actionNode.Namespace = "default"
	}

	a.Type = actionNode.Type
	a.ConfigMapName = actionNode.ConfigMapName
	a.Namespace = actionNode.Namespace
	a.Key = actionNode.Key
	a.Sum = actionNode.Sum
	a.Members = actionNode.Members

	return nil
}

func UnmarshalActionEcho(unmarshal func(interface{}) error, a *ActionEchoData) error {
	type Alias ActionEchoData
	aux := (*Alias)(a)
	return unmarshal(aux)
}

func UnmarshalActionDelay(unmarshal func(interface{}) error, a *ActionDelayData) error {
	type Alias ActionDelayData
	aux := (*Alias)(a)
	return unmarshal(aux)
}

func UnmarshalActionK8sExecDeployment(unmarshal func(interface{}) error, a *ActionK8sExecDeploymentData) error {
	type Alias ActionK8sExecDeploymentData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func UnmarshalActionK8sRestartDeployment(unmarshal func(interface{}) error, a *ActionK8sRestartDeploymentData) error {
	type Alias ActionK8sRestartDeploymentData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func UnmarshalActionK8sWaitDeploymentRollout(unmarshal func(interface{}) error, a *ActionK8sWaitDeploymentRolloutData) error {
	type Alias ActionK8sWaitDeploymentRolloutData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	if a.Timeout == 0 {
		a.Timeout = 5 * time.Minute
	}
	return nil
}

func UnmarshalActionK8sUpdateConfigMap(unmarshal func(interface{}) error, a *ActionK8sUpdateConfigMapData) error {
	type Alias ActionK8sUpdateConfigMapData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

func UnmarshalActionK8sScaleDeployment(unmarshal func(interface{}) error, a *ActionK8sScaleDeploymentData) error {
	type Alias ActionK8sScaleDeploymentData
	aux := (*Alias)(a)
	if err := unmarshal(aux); err != nil {
		return err
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}
