package task

import "time"

// ConditionType identifies the type of condition
type ConditionType string

const (
	ConditionTypeEndpointSuccess    ConditionType = "endpoint_success"
	ConditionTypeEndpointValue      ConditionType = "endpoint_value"
	ConditionTypePrometheusMetric   ConditionType = "prometheus_metric"
	ConditionTypeAlwaysTrue         ConditionType = "always_true"
	ConditionTypeK8sDeploymentReady ConditionType = "k8s_deployment_ready"
)

// ConditionEndpointSuccess checks if an HTTP endpoint returns expected status/response
type ConditionEndpointSuccess struct {
	Type         ConditionType `yaml:"type"`
	Endpoint     string        `yaml:"endpoint"`
	ResponseBody string        `yaml:"response,omitempty"`
	Status       int           `yaml:"status,omitempty"`
}

// ConditionAlwaysTrue always evaluates to true (useful for testing/always-run tasks)
type ConditionAlwaysTrue struct {
	Type ConditionType `yaml:"type"`
}

// ConditionEndpointValue compares an integer value from an endpoint
type ConditionEndpointValue struct {
	Type     ConditionType `yaml:"type"`
	Endpoint string        `yaml:"endpoint"`
	Value    int           `yaml:"value"`
	Operator string        `yaml:"operator"` // eq, ne, lt, gt, le, ge
}

// ConditionPrometheusMetric compares a Prometheus metric value
type ConditionPrometheusMetric struct {
	Type       ConditionType `yaml:"type"`
	Endpoint   string        `yaml:"endpoint"`
	MetricName string        `yaml:"metric_name"`
	Value      float64       `yaml:"value"`
	Operator   string        `yaml:"operator"` // eq, ne, lt, gt, le, ge
}

// ConditionK8sDeploymentReady checks if a K8s deployment has expected replicas ready
type ConditionK8sDeploymentReady struct {
	Type       ConditionType `yaml:"type"`
	Member     string        `yaml:"member"`
	Deployment string        `yaml:"deployment"`
	Namespace  string        `yaml:"namespace,omitempty"`
	Replicas   int32         `yaml:"replicas"`
}

// ActionType identifies the type of action
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

// ActionEndpoint makes an HTTP request to an endpoint
type ActionEndpoint struct {
	Type     ActionType        `yaml:"type"`
	Endpoint string            `yaml:"endpoint"`
	Method   string            `yaml:"method"` // GET, POST, PUT, DELETE
	Headers  map[string]string `yaml:"headers,omitempty"`
	Body     string            `yaml:"body,omitempty"`
}

// ActionEcho logs a message
type ActionEcho struct {
	Type    ActionType `yaml:"type"`
	Message string     `yaml:"message"`
}

// ActionDelay pauses execution for a duration
type ActionDelay struct {
	Type ActionType    `yaml:"type"`
	Time time.Duration `yaml:"time"`
}

// ActionConfigValueSum distributes a sum across multiple cluster configmaps
type ActionConfigValueSum struct {
	Type          ActionType `yaml:"type"`
	ConfigMapName string     `yaml:"config_map"`
	Namespace     string     `yaml:"namespace,omitempty"`
	Key           string     `yaml:"key"`
	Sum           int        `yaml:"sum"`
	Members       []string   `yaml:"members"`
}

// ActionK8sExecDeployment executes a command on deployment pods
type ActionK8sExecDeployment struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
	Container  string     `yaml:"container,omitempty"`
	Command    []string   `yaml:"command"`
}

// ActionK8sRestartDeployment triggers a deployment restart
type ActionK8sRestartDeployment struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
}

// ActionK8sWaitDeploymentRollout waits for a deployment rollout to complete
type ActionK8sWaitDeploymentRollout struct {
	Type       ActionType    `yaml:"type"`
	Member     string        `yaml:"member"`
	Deployment string        `yaml:"deployment"`
	Namespace  string        `yaml:"namespace,omitempty"`
	Timeout    time.Duration `yaml:"timeout,omitempty"` // Default: 5 minutes
}

// ActionK8sUpdateConfigMap updates a configmap key
type ActionK8sUpdateConfigMap struct {
	Type      ActionType `yaml:"type"`
	Member    string     `yaml:"member"`
	ConfigMap string     `yaml:"config_map"`
	Namespace string     `yaml:"namespace,omitempty"`
	Key       string     `yaml:"key"`
	Value     string     `yaml:"value"`
}

// ActionK8sScaleDeployment scales a deployment to specified replicas
type ActionK8sScaleDeployment struct {
	Type       ActionType `yaml:"type"`
	Member     string     `yaml:"member"`
	Deployment string     `yaml:"deployment"`
	Namespace  string     `yaml:"namespace,omitempty"`
	Replicas   int32      `yaml:"replicas"`
}
