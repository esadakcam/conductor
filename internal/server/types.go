package server

import "fmt"

// WriteRequest is the common structure for write operations requiring epoch validation
type WriteRequest struct {
	Epoch int64 `json:"epoch"`
	// Body will be parsed separately based on the operation if needed,
	// or the whole request body can be decoded into a struct embedding this.
	// However, for K8s operations, we often receive the object definition itself.
	// The plan says "Write operation requests include epoch field in JSON body".
	// If we want to allow sending a K8s manifest AND an epoch, it might be better
	// to wrap it or expect the epoch in a specific way.
	// For simplicity and strict adherence to the plan, let's assume the body IS the object
	// but with an extra field, or we use a wrapper.
	// Given standard K8s objects don't have "epoch", a wrapper is safer.
	// But let's look at the plan again: "Write operation requests include epoch field in JSON body".
	// Let's define a wrapper.
}

// CreateRequest wraps the object creation payload
type CreateRequest struct {
	Epoch  int64       `json:"epoch"`
	Object interface{} `json:"object"` // The K8s object payload
}

// UpdateRequest wraps the object update payload
type UpdateRequest struct {
	Epoch  int64       `json:"epoch"`
	Object interface{} `json:"object"`
}

// PatchRequest wraps the patch payload
type PatchRequest struct {
	Epoch int64  `json:"epoch"`
	Patch []byte `json:"patch"` // Raw patch data
}

// DeleteRequest mainly needs epoch
type DeleteRequest struct {
	Epoch int64 `json:"epoch"`
}

// ErrorResponse defines the standard error format
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// AppError represents a structured error within the application
type AppError struct {
	Message string
	Code    int
	Err     error
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func NewAppError(code int, message string, err error) *AppError {
	return &AppError{
		Message: message,
		Code:    code,
		Err:     err,
	}
}

// ServerConfig holds the server configuration
type ServerConfig struct {
	Port      int      `yaml:"port"`
	Followers []string `yaml:"followers"`
}
