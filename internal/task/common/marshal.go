package common

import "github.com/esadakcam/conductor/internal/task"

// UnmarshalYAML implementations for condition types

func (c *ConditionEndpointSuccess) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalConditionEndpointSuccess(unmarshal, &c.ConditionEndpointSuccessData)
}

func (c *ConditionEndpointValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalConditionEndpointValue(unmarshal, &c.ConditionEndpointValueData)
}

func (c *ConditionPrometheusMetric) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalConditionPrometheusMetric(unmarshal, &c.ConditionPrometheusMetricData)
}

func (c *ConditionAlwaysTrue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalConditionAlwaysTrue(unmarshal, &c.ConditionAlwaysTrueData)
}

// UnmarshalYAML implementations for action types

func (a *ActionEndpoint) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionEndpoint(unmarshal, &a.ActionEndpointData)
}

func (a *ActionEcho) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionEcho(unmarshal, &a.ActionEchoData)
}

func (a *ActionDelay) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionDelay(unmarshal, &a.ActionDelayData)
}
