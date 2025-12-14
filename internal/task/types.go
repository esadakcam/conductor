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
	ActionTypeEndpoint             ActionType = "endpoint"
	ActionTypeEcho                 ActionType = "echo"
	ActionTypeConfigValueSum       ActionType = "config_value_sum"
	ActionTypeK8sExecDeployment    ActionType = "k8s_exec_deployment"
	ActionTypeK8sRestartDeployment ActionType = "k8s_restart_deployment"
)

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

type Task struct {
	Name string      `yaml:"name"`
	When []Condition `yaml:"when"`
	Then []Action    `yaml:"then"`
}

type Condition interface {
	Evaluate(ctx context.Context) (bool, error)
	GetType() ConditionType
}

type Action interface {
	Execute(ctx context.Context, epoch int64, idempotencyId string) error
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

type ActionConfigValueSum struct {
	Type          ActionType `yaml:"type"`
	ConfigMapName string     `yaml:"configMap"`
	Key           string     `yaml:"key"`
	Sum           int        `yaml:"sum"`
	Members       []string   `yaml:"members"` // todo make it a struct and pass auth info
}

type ActionK8sExecDeployment struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
	Container  string     `yaml:"container,omitempty"`
	Command    []string   `yaml:"command"`
}

type ActionK8sRestartDeployment struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
}

// TODO: Add k8s related actions
