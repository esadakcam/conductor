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

	// 2. Test List format (Root is list)
	listContent := `
- name: task2
  when:
    - type: always_true
  then:
    - type: echo
      message: "hello"
`
	listFile, err := os.CreateTemp("", "config_list_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(listFile.Name())
	if _, err := listFile.WriteString(listContent); err != nil {
		t.Fatal(err)
	}
	listFile.Close()

	tasksList, err := LoadDistributedTasks(listFile.Name())
	if err != nil {
		t.Fatalf("Failed to load tasks from list: %v", err)
	}
	if len(tasksList) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasksList))
	}
	if tasksList[0].GetName() != "task2" {
		t.Errorf("Expected task name 'task2', got '%s'", tasksList[0].GetName())
	}

	// 3. Test UnmarshalYAML trigger (Polymorphism)
	// We check if conditions are populated, which requires UnmarshalYAML to work
	if len(tasks[0].GetConditions()) == 0 {
		t.Error("Conditions not loaded, UnmarshalYAML might not have been triggered")
	}
}
