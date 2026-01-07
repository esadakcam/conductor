package utils

import (
	"os"
	"testing"
)

func TestLoadDistributedTasks(t *testing.T) {
	// 1. Test Config Object format (Root object with "tasks" key)
	configContent := `
name: test-config
tasks:
  - name: task1
    when:
      - type: always_true
    then:
      - type: echo
        message: "hello"
`
	configFile, err := os.CreateTemp("", "config_object_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(configFile.Name())
	if _, err := configFile.WriteString(configContent); err != nil {
		t.Fatal(err)
	}
	configFile.Close()

	tasks, err := LoadDistributedTasks(configFile.Name())
	if err != nil {
		t.Fatalf("Failed to load tasks from config object: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}
	if tasks[0].GetName() != "task1" {
		t.Errorf("Expected task name 'task1', got '%s'", tasks[0].GetName())
	}
}
