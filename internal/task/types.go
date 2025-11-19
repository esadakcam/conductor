package task

import "context"

type ConditionType string

const (
	ConditionTypeEndpointSuccess ConditionType = "endpoint_success"
	ConditionTypeEndpointValue   ConditionType = "endpoint_value"
	ConditionTypeAlwaysTrue      ConditionType = "always_true"
)

type ActionType string

const (
	ActionTypeEndpoint ActionType = "endpoint"
	ActionTypeEcho     ActionType = "echo"
)

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

type Task struct {
	When Condition `yaml:"when"`
	Then Action    `yaml:"then"`
}

type Condition interface {
	Evaluate(ctx context.Context) (bool, error)
	GetType() ConditionType
}

type Action interface {
	Execute(ctx context.Context, epoch int64) error
	GetType() ActionType
}

type ConditionEndpointSuccess struct {
	Type         ConditionType `yaml:"type"`
	Endpoint     string        `yaml:"endpoint"`
	ResponseBody string        `yaml:"response,omitempty"`
	Status       int           `yaml:"status,omitempty"`
}

type ConditionAlwaysTrue struct {
	Type ConditionType `yaml:"type"`
}

type ConditionEndpointValue struct {
	Type     ConditionType `yaml:"type"`
	Endpoint string        `yaml:"endpoint"`
	Value    int           `yaml:"value"`
	Operator string        `yaml:"operator"` // eq, ne, lt, gt, le, ge
}

type ActionEndpoint struct {
	Type     ActionType        `yaml:"type"`
	Endpoint string            `yaml:"endpoint"`
	Method   string            `yaml:"method"` // GET, POST, PUT, DELETE
	Headers  map[string]string `yaml:"headers,omitempty"`
	Body     string            `yaml:"body,omitempty"`
}

type ActionEcho struct {
	Type    ActionType `yaml:"type"`
	Message string     `yaml:"message"`
}

// TODO: Add k8s related actions
