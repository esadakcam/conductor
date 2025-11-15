package task

type ConditionType string

const (
	ConditionTypeEndpoint ConditionType = "endpoint"
)

type ActionType string

const (
	ActionTypeEndpoint ActionType = "endpoint"
)

// Config represents the root configuration structure
type Config struct {
	Tasks []Task `yaml:"tasks"`
}

// Task represents a single task with a condition and action
type Task struct {
	When Condition `yaml:"when"`
	Then Action    `yaml:"then"`
}

// Condition represents the "when" part of a task
type Condition interface {
	Evaluate() (bool, error)
	GetType() ConditionType
}

// Action represents the "then" part of a task
type Action interface {
	Execute() error
	GetType() ActionType
}

type EndpointCondition struct {
	Type     ConditionType `yaml:"type"`
	Endpoint string        `yaml:"endpoint,omitempty"`
	Operator string        `yaml:"operator,omitempty"` // "<", ">", "==", "!=", "<=", ">="
	Value    interface{}   `yaml:"value,omitempty"`
}

type EndpointAction struct {
	Type     ActionType        `yaml:"type"`
	Endpoint string            `yaml:"endpoint"`
	Method   string            `yaml:"method"` // GET, POST, PUT, DELETE (default: GET)
	Headers  map[string]string `yaml:"headers,omitempty"`
	Body     string            `yaml:"body,omitempty"`
}
