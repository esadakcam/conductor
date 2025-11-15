package task

type ConditionType string

const (
	ConditionTypeEndpointSuccess ConditionType = "endpoint_success"
	ConditionTypeEndpointValue   ConditionType = "endpoint_value"
)

type ActionType string

const (
	ActionTypeEndpoint ActionType = "endpoint"
)

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

type Task struct {
	When Condition `yaml:"when"`
	Then Action    `yaml:"then"`
}

type Condition interface {
	Evaluate() (bool, error)
	GetType() ConditionType
}

type Action interface {
	Execute() error
	GetType() ActionType
}

type ConditionEndpointSuccess struct {
	Type         ConditionType `yaml:"type"`
	Endpoint     string        `yaml:"endpoint"`
	ResponseBody string        `yaml:"response,omitempty"`
	Status       int           `yaml:"status,omitempty"`
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

// TODO: Add k8s related actions
