package common

import "github.com/esadakcam/conductor/internal/task"

// Condition types wrapping base types
// These are shared across centralized and distributed modes.
type ConditionEndpointSuccess struct {
	task.ConditionEndpointSuccessData
}

type ConditionAlwaysTrue struct {
	task.ConditionAlwaysTrueData
}

type ConditionEndpointValue struct {
	task.ConditionEndpointValueData
}

type ConditionPrometheusMetric struct {
	task.ConditionPrometheusMetricData
}

// Action types wrapping base types
// These are shared across centralized and distributed modes.
type ActionEndpoint struct {
	task.ActionEndpointData
}

type ActionEcho struct {
	task.ActionEchoData
}

type ActionDelay struct {
	task.ActionDelayData
}
