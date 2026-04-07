package task

import (
	"context"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
)

// ExecutionContext provides access to execution-time resources for actions and conditions.
// It abstracts the differences between centralized mode (direct k8s clients) and
// distributed mode (epoch-based coordination via HTTP).
type ExecutionContext interface {
	GetEpoch() int64
	GetIdempotencyKey() string
	GetK8sClients() map[string]*k8s.Client
}

type ConditionType string

const (
	ConditionTypeEndpointSuccess    ConditionType = "endpoint_success"
	ConditionTypeEndpointValue      ConditionType = "endpoint_value"
	ConditionTypePrometheusMetric   ConditionType = "prometheus_metric"
	ConditionTypeAlwaysTrue         ConditionType = "always_true"
	ConditionTypeK8sDeploymentReady ConditionType = "k8s_deployment_ready"
)

type ActionType string

const (
	ActionTypeEndpoint                 ActionType = "endpoint"
	ActionTypeEcho                     ActionType = "echo"
	ActionTypeDelay                    ActionType = "delay"
	ActionTypeConfigValueSum           ActionType = "config_value_sum"
	ActionTypeK8sExecDeployment        ActionType = "k8s_exec_deployment"
	ActionTypeK8sRestartDeployment     ActionType = "k8s_restart_deployment"
	ActionTypeK8sWaitDeploymentRollout ActionType = "k8s_wait_deployment_rollout"
	ActionTypeK8sUpdateConfigMap       ActionType = "k8s_update_configmap"
	ActionTypeK8sScaleDeployment       ActionType = "k8s_scale_deployment"
)

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

// Task is the concrete task struct used by both centralized and distributed modes
type Task struct {
	Name string      `yaml:"name"`
	When []Condition `yaml:"when"`
	Then []Action    `yaml:"then"`
}

func (t *Task) GetName() string {
	return t.Name
}

func (t *Task) GetConditions() []Condition {
	return t.When
}

func (t *Task) GetActions() []Action {
	return t.Then
}

// TaskInterface is the interface for tasks used by the outbox
type TaskInterface interface {
	GetName() string
	GetConditions() []Condition
	GetActions() []Action
}

type Condition interface {
	Evaluate(ctx context.Context, ec ExecutionContext) (bool, error)
	GetType() ConditionType
}

type Action interface {
	Execute(ctx context.Context, ec ExecutionContext) error
	GetType() ActionType
}

// Base condition structs
type ConditionEndpointSuccessData struct {
	Type         ConditionType `yaml:"type"`
	Endpoint     string        `yaml:"endpoint"`
	ResponseBody string        `yaml:"response,omitempty"`
	Status       int           `yaml:"status,omitempty"`
}

type ConditionAlwaysTrueData struct {
	Type ConditionType `yaml:"type"`
}

type ConditionEndpointValueData struct {
	Type     ConditionType `yaml:"type"`
	Endpoint string        `yaml:"endpoint"`
	Value    int           `yaml:"value"`
	Operator string        `yaml:"operator"` // eq, ne, lt, gt, le, ge
}

type ConditionPrometheusMetricData struct {
	Type       ConditionType `yaml:"type"`
	Endpoint   string        `yaml:"endpoint"`
	MetricName string        `yaml:"metric_name"`
	Value      float64       `yaml:"value"`
	Operator   string        `yaml:"operator"` // eq, ne, lt, gt, le, ge
}

type ConditionK8sDeploymentReadyData struct {
	Type       ConditionType `yaml:"type"`
	Member     string        `yaml:"member"`
	Deployment string        `yaml:"deployment"`
	Namespace  string        `yaml:"namespace,omitempty"`
	Replicas   int32         `yaml:"replicas"`
}

// Base action structs
type ActionEndpointData struct {
	Type     ActionType        `yaml:"type"`
	Endpoint string            `yaml:"endpoint"`
	Method   string            `yaml:"method"` // GET, POST, PUT, DELETE
	Headers  map[string]string `yaml:"headers,omitempty"`
	Body     string            `yaml:"body,omitempty"`
}

type ActionEchoData struct {
	Type    ActionType `yaml:"type"`
	Message string     `yaml:"message"`
}

type ActionDelayData struct {
	Type ActionType    `yaml:"type"`
	Time time.Duration `yaml:"time"`
}

type ActionConfigValueSumData struct {
	Type          ActionType `yaml:"type"`
	ConfigMapName string     `yaml:"configMap"`
	Namespace     string     `yaml:"namespace,omitempty"`
	Key           string     `yaml:"key"`
	Sum           int        `yaml:"sum"`
	Members       []string   `yaml:"members"` // todo make it a struct and pass auth info
}

// ActionK8sExecDeploymentData executes a command on all pods of a deployment.
//
// In centralized mode, this action provides at-least-once semantics. Idempotency
// is tracked per-pod via the conductor.io/idempotency-key label, but because
// Kubernetes does not support an atomic exec-and-patch operation, there is an
// unavoidable window between a successful exec and the subsequent label patch.
// If the controller crashes in that window, the command will be re-executed on
// the next retry. Commands used with this action MUST be idempotent.
type ActionK8sExecDeploymentData struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
	Container  string     `yaml:"container,omitempty"`
	Command    []string   `yaml:"command"`
}

type ActionK8sRestartDeploymentData struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
}

type ActionK8sWaitDeploymentRolloutData struct {
	Type       ActionType    `yaml:"type"`
	Member     string        `yaml:"member"`
	Deployment string        `yaml:"deployment"`
	Namespace  string        `yaml:"namespace,omitempty"`
	Timeout    time.Duration `yaml:"timeout,omitempty"` // Default: 5 minutes
}

type ActionK8sUpdateConfigMapData struct {
	Type      ActionType `yaml:"type"`
	Member    string     `yaml:"member"`
	ConfigMap string     `yaml:"config_map"`
	Namespace string     `yaml:"namespace,omitempty"`
	Key       string     `yaml:"key"`
	Value     string     `yaml:"value"`
}

type ActionK8sScaleDeploymentData struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
	Replicas   int32      `yaml:"replicas"`
}
