package task

import (
	"context"
)

// Condition evaluates whether a task should be executed
type Condition interface {
	Evaluate(ctx context.Context, execCtx ExecutionContext) (bool, error)
	GetType() ConditionType
}

// Action performs an operation when a task is triggered
type Action interface {
	Execute(ctx context.Context, execCtx ExecutionContext) error
	GetType() ActionType
}

// TaskInterface defines the contract for tasks
type TaskInterface interface {
	GetName() string
	GetConditions() []Condition
	GetActions() []Action
}

// ExecutionContext provides the execution environment for actions and conditions.
// This replaces the generic `payload any` with a typed interface.
type ExecutionContext interface {
	// GetMode returns the execution mode (centralized or distributed)
	GetMode() Mode

	// GetIdempotencyID returns the idempotency ID for distributed mode
	GetIdempotencyID() string

	// GetEpoch returns the current epoch for distributed mode
	GetEpoch() int64
}

// Mode represents the conductor execution mode
type Mode string

const (
	ModeCentralized Mode = "centralized"
	ModeDistributed Mode = "distributed"
)

// Task is the concrete task implementation
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

// Config represents the tasks configuration
type Config struct {
	Tasks []Task `yaml:"tasks"`
}
