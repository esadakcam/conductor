package task

import (
	"context"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// mockCondition implements the Condition interface for testing
type mockCondition struct {
	ConditionEndpointSuccessData
}

func (m *mockCondition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalConditionEndpointSuccess(unmarshal, &m.ConditionEndpointSuccessData)
}
func (m *mockCondition) Evaluate(ctx context.Context, ec ExecutionContext) (bool, error) {
	return true, nil
}
func (m *mockCondition) GetType() ConditionType                                  { return ConditionTypeEndpointSuccess }

// mockAlwaysTrueCondition implements the Condition interface for testing
type mockAlwaysTrueCondition struct {
	ConditionAlwaysTrueData
}

func (m *mockAlwaysTrueCondition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalConditionAlwaysTrue(unmarshal, &m.ConditionAlwaysTrueData)
}
func (m *mockAlwaysTrueCondition) Evaluate(ctx context.Context, ec ExecutionContext) (bool, error) {
	return true, nil
}
func (m *mockAlwaysTrueCondition) GetType() ConditionType { return ConditionTypeAlwaysTrue }

// mockEndpointValueCondition implements the Condition interface for testing
type mockEndpointValueCondition struct {
	ConditionEndpointValueData
}

func (m *mockEndpointValueCondition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalConditionEndpointValue(unmarshal, &m.ConditionEndpointValueData)
}
func (m *mockEndpointValueCondition) Evaluate(ctx context.Context, ec ExecutionContext) (bool, error) {
	return true, nil
}
func (m *mockEndpointValueCondition) GetType() ConditionType { return ConditionTypeEndpointValue }

// mockPrometheusCondition implements the Condition interface for testing
type mockPrometheusCondition struct {
	ConditionPrometheusMetricData
}

func (m *mockPrometheusCondition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalConditionPrometheusMetric(unmarshal, &m.ConditionPrometheusMetricData)
}
func (m *mockPrometheusCondition) Evaluate(ctx context.Context, ec ExecutionContext) (bool, error) {
	return true, nil
}
func (m *mockPrometheusCondition) GetType() ConditionType { return ConditionTypePrometheusMetric }

// mockK8sDeploymentReadyCondition implements the Condition interface for testing
type mockK8sDeploymentReadyCondition struct {
	ConditionK8sDeploymentReadyData
}

func (m *mockK8sDeploymentReadyCondition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalConditionK8sDeploymentReady(unmarshal, &m.ConditionK8sDeploymentReadyData)
}
func (m *mockK8sDeploymentReadyCondition) Evaluate(ctx context.Context, ec ExecutionContext) (bool, error) {
	return true, nil
}
func (m *mockK8sDeploymentReadyCondition) GetType() ConditionType {
	return ConditionTypeK8sDeploymentReady
}

// mockAction implements the Action interface for testing
type mockAction struct {
	ActionEndpointData
}

func (m *mockAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionEndpoint(unmarshal, &m.ActionEndpointData)
}
func (m *mockAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockAction) GetType() ActionType                            { return ActionTypeEndpoint }

// mockEchoAction implements the Action interface for testing
type mockEchoAction struct {
	ActionEchoData
}

func (m *mockEchoAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionEcho(unmarshal, &m.ActionEchoData)
}
func (m *mockEchoAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockEchoAction) GetType() ActionType                            { return ActionTypeEcho }

// mockDelayAction implements the Action interface for testing
type mockDelayAction struct {
	ActionDelayData
}

func (m *mockDelayAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionDelay(unmarshal, &m.ActionDelayData)
}
func (m *mockDelayAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockDelayAction) GetType() ActionType                            { return ActionTypeDelay }

// mockConfigValueSumAction implements the Action interface for testing
type mockConfigValueSumAction struct {
	ActionConfigValueSumData
}

func (m *mockConfigValueSumAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionConfigValueSum(unmarshal, &m.ActionConfigValueSumData)
}
func (m *mockConfigValueSumAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockConfigValueSumAction) GetType() ActionType                            { return ActionTypeConfigValueSum }

// mockK8sExecDeploymentAction implements the Action interface for testing
type mockK8sExecDeploymentAction struct {
	ActionK8sExecDeploymentData
}

func (m *mockK8sExecDeploymentAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionK8sExecDeployment(unmarshal, &m.ActionK8sExecDeploymentData)
}
func (m *mockK8sExecDeploymentAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockK8sExecDeploymentAction) GetType() ActionType {
	return ActionTypeK8sExecDeployment
}

// mockK8sRestartDeploymentAction implements the Action interface for testing
type mockK8sRestartDeploymentAction struct {
	ActionK8sRestartDeploymentData
}

func (m *mockK8sRestartDeploymentAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionK8sRestartDeployment(unmarshal, &m.ActionK8sRestartDeploymentData)
}
func (m *mockK8sRestartDeploymentAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockK8sRestartDeploymentAction) GetType() ActionType {
	return ActionTypeK8sRestartDeployment
}

// mockK8sWaitDeploymentRolloutAction implements the Action interface for testing
type mockK8sWaitDeploymentRolloutAction struct {
	ActionK8sWaitDeploymentRolloutData
}

func (m *mockK8sWaitDeploymentRolloutAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionK8sWaitDeploymentRollout(unmarshal, &m.ActionK8sWaitDeploymentRolloutData)
}
func (m *mockK8sWaitDeploymentRolloutAction) Execute(ctx context.Context, ec ExecutionContext) error {
	return nil
}
func (m *mockK8sWaitDeploymentRolloutAction) GetType() ActionType {
	return ActionTypeK8sWaitDeploymentRollout
}

// mockK8sUpdateConfigMapAction implements the Action interface for testing
type mockK8sUpdateConfigMapAction struct {
	ActionK8sUpdateConfigMapData
}

func (m *mockK8sUpdateConfigMapAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionK8sUpdateConfigMap(unmarshal, &m.ActionK8sUpdateConfigMapData)
}
func (m *mockK8sUpdateConfigMapAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockK8sUpdateConfigMapAction) GetType() ActionType {
	return ActionTypeK8sUpdateConfigMap
}

// mockK8sScaleDeploymentAction implements the Action interface for testing
type mockK8sScaleDeploymentAction struct {
	ActionK8sScaleDeploymentData
}

func (m *mockK8sScaleDeploymentAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return UnmarshalActionK8sScaleDeployment(unmarshal, &m.ActionK8sScaleDeploymentData)
}
func (m *mockK8sScaleDeploymentAction) Execute(ctx context.Context, ec ExecutionContext) error { return nil }
func (m *mockK8sScaleDeploymentAction) GetType() ActionType {
	return ActionTypeK8sScaleDeployment
}

// testFactory implements TypeFactory for testing
type testFactory struct{}

func (f testFactory) NewConditionEndpointSuccess() Condition  { return &mockCondition{} }
func (f testFactory) NewConditionEndpointValue() Condition    { return &mockEndpointValueCondition{} }
func (f testFactory) NewConditionPrometheusMetric() Condition { return &mockPrometheusCondition{} }
func (f testFactory) NewConditionAlwaysTrue() Condition       { return &mockAlwaysTrueCondition{} }
func (f testFactory) NewConditionK8sDeploymentReady() Condition {
	return &mockK8sDeploymentReadyCondition{}
}
func (f testFactory) NewActionEndpoint() Action             { return &mockAction{} }
func (f testFactory) NewActionEcho() Action                 { return &mockEchoAction{} }
func (f testFactory) NewActionDelay() Action                { return &mockDelayAction{} }
func (f testFactory) NewActionConfigValueSum() Action       { return &mockConfigValueSumAction{} }
func (f testFactory) NewActionK8sExecDeployment() Action    { return &mockK8sExecDeploymentAction{} }
func (f testFactory) NewActionK8sRestartDeployment() Action { return &mockK8sRestartDeploymentAction{} }
func (f testFactory) NewActionK8sWaitDeploymentRollout() Action {
	return &mockK8sWaitDeploymentRolloutAction{}
}
func (f testFactory) NewActionK8sUpdateConfigMap() Action { return &mockK8sUpdateConfigMapAction{} }
func (f testFactory) NewActionK8sScaleDeployment() Action { return &mockK8sScaleDeploymentAction{} }

// configRaw is used for the first pass of unmarshalling
type configRaw struct {
	Tasks []yaml.Node `yaml:"tasks"`
}

func unmarshalTasks(data []byte, factory TypeFactory) ([]Task, error) {
	var rawConfig configRaw
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		return nil, err
	}

	tasks := make([]Task, 0, len(rawConfig.Tasks))
	for i, taskNode := range rawConfig.Tasks {
		t, err := UnmarshalTaskFromNode(&taskNode, i, factory)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *t)
	}
	return tasks, nil
}

func TestUnmarshalCentralizedConfig(t *testing.T) {
	configPath := "../../config/config.centralized.yaml"
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Skipf("Skipping test: config file not found at %s", configPath)
		return
	}

	factory := testFactory{}
	tasks, err := unmarshalTasks(data, factory)
	if err != nil {
		t.Fatalf("Failed to unmarshal centralized config: %v", err)
	}

	if len(tasks) == 0 {
		t.Fatal("Expected at least one task")
	}

	// Verify first task
	task := tasks[0]
	if task.Name == "" {
		t.Error("Task name should not be empty")
	}

	t.Logf("Successfully parsed %d tasks from centralized config", len(tasks))
	for i, task := range tasks {
		t.Logf("Task %d: %s with %d conditions and %d actions",
			i, task.Name, len(task.When), len(task.Then))
	}
}

func TestUnmarshalDistributedConfig(t *testing.T) {
	configPath := "../../config/config.distributed.yaml"
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Skipf("Skipping test: config file not found at %s", configPath)
		return
	}

	factory := testFactory{}
	tasks, err := unmarshalTasks(data, factory)
	if err != nil {
		t.Fatalf("Failed to unmarshal distributed config: %v", err)
	}

	if len(tasks) == 0 {
		t.Fatal("Expected at least one task")
	}

	// Verify first task
	task := tasks[0]
	if task.Name == "" {
		t.Error("Task name should not be empty")
	}

	t.Logf("Successfully parsed %d tasks from distributed config", len(tasks))
	for i, task := range tasks {
		t.Logf("Task %d: %s with %d conditions and %d actions",
			i, task.Name, len(task.When), len(task.Then))
	}
}

func TestConditionDefaultValues(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		validate func(t *testing.T, condition Condition)
	}{
		{
			name: "ConditionEndpointSuccess defaults status to 200",
			yaml: `type: endpoint_success
endpoint: http://example.com`,
			validate: func(t *testing.T, condition Condition) {
				c := condition.(*mockCondition)
				if c.Status != 200 {
					t.Errorf("Expected default status 200, got %d", c.Status)
				}
			},
		},
		{
			name: "ConditionEndpointValue defaults operator to eq",
			yaml: `type: endpoint_value
endpoint: http://example.com
value: 5`,
			validate: func(t *testing.T, condition Condition) {
				c := condition.(*mockEndpointValueCondition)
				if c.Operator != "eq" {
					t.Errorf("Expected default operator 'eq', got %s", c.Operator)
				}
			},
		},
		{
			name: "ConditionK8sDeploymentReady defaults namespace to default",
			yaml: `type: k8s_deployment_ready
member: http://localhost:8080
deployment: nginx
replicas: 1`,
			validate: func(t *testing.T, condition Condition) {
				c := condition.(*mockK8sDeploymentReadyCondition)
				if c.Namespace != "default" {
					t.Errorf("Expected default namespace 'default', got %s", c.Namespace)
				}
			},
		},
	}

	factory := testFactory{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(tt.yaml), &node); err != nil {
				t.Fatalf("Failed to parse YAML: %v", err)
			}

			condition, err := DecodeCondition(&node, 0, factory)
			if err != nil {
				t.Fatalf("Failed to decode condition: %v", err)
			}

			tt.validate(t, condition)
		})
	}
}

func TestActionDefaultValues(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		validate func(t *testing.T, action Action)
	}{
		{
			name: "ActionEndpoint defaults method to GET",
			yaml: `type: endpoint
endpoint: http://example.com`,
			validate: func(t *testing.T, action Action) {
				a := action.(*mockAction)
				if a.Method != "GET" {
					t.Errorf("Expected default method 'GET', got %s", a.Method)
				}
			},
		},
		{
			name: "ActionK8sExecDeployment defaults namespace to default",
			yaml: `type: k8s_exec_deployment
member: http://localhost:8080
deployment: nginx
command: ["ls", "-la"]`,
			validate: func(t *testing.T, action Action) {
				a := action.(*mockK8sExecDeploymentAction)
				if a.Namespace != "default" {
					t.Errorf("Expected default namespace 'default', got %s", a.Namespace)
				}
			},
		},
		{
			name: "ActionK8sRestartDeployment defaults namespace to default",
			yaml: `type: k8s_restart_deployment
member: http://localhost:8080
deployment: nginx`,
			validate: func(t *testing.T, action Action) {
				a := action.(*mockK8sRestartDeploymentAction)
				if a.Namespace != "default" {
					t.Errorf("Expected default namespace 'default', got %s", a.Namespace)
				}
			},
		},
	}

	factory := testFactory{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(tt.yaml), &node); err != nil {
				t.Fatalf("Failed to parse YAML: %v", err)
			}

			action, err := DecodeAction(&node, 0, factory)
			if err != nil {
				t.Fatalf("Failed to decode action: %v", err)
			}

			tt.validate(t, action)
		})
	}
}

func TestConfigValueSumAction(t *testing.T) {
	yamlData := `type: config_value_sum
config_map: nginx-data
key: value
sum: 920
members:
  - "http://localhost:8080"
  - "http://localhost:8081"`

	var node yaml.Node
	if err := yaml.Unmarshal([]byte(yamlData), &node); err != nil {
		t.Fatalf("Failed to parse YAML: %v", err)
	}

	factory := testFactory{}
	action, err := DecodeAction(&node, 0, factory)
	if err != nil {
		t.Fatalf("Failed to decode action: %v", err)
	}

	a := action.(*mockConfigValueSumAction)
	if a.ConfigMapName != "nginx-data" {
		t.Errorf("Expected ConfigMapName 'nginx-data', got %s", a.ConfigMapName)
	}
	if a.Key != "value" {
		t.Errorf("Expected Key 'value', got %s", a.Key)
	}
	if a.Sum != 920 {
		t.Errorf("Expected Sum 920, got %d", a.Sum)
	}
	if len(a.Members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(a.Members))
	}
}
