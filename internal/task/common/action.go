package common

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func (a *ActionEndpoint) Execute(ctx context.Context, ec task.ExecutionContext) error {
	idempotencyId := ec.GetIdempotencyKey()

	if a.Endpoint == "" {
		err := fmt.Errorf("endpoint is required")
		logger.Error("ActionEndpoint: endpoint is required")
		return err
	}

	// Default to GET if method is not specified
	method := a.Method
	if method == "" {
		method = "GET"
	}

	// Create request body if provided
	var reqBody io.Reader
	if a.Body != "" {
		reqBody = bytes.NewBufferString(a.Body)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, method, a.Endpoint, reqBody)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to create request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers if provided
	for key, value := range a.Headers {
		req.Header.Set(key, value)
	}
	if idempotencyId != "" {
		req.Header.Set("X-Idempotency-Id", idempotencyId)
	}

	// Set Content-Type header if body is provided and Content-Type is not already set
	if a.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Execute request
	client := httpclient.Get()

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to execute request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to execute request to %s: %w", a.Endpoint, err)
	}
	defer resp.Body.Close()

	// Check for HTTP error status codes (4xx, 5xx)
	if resp.StatusCode >= 400 {
		err := fmt.Errorf("request failed with status code %d", resp.StatusCode)
		logger.Errorf("ActionEndpoint: request to %s failed with status code %d", a.Endpoint, resp.StatusCode)
		return err
	}

	return nil
}

func (a *ActionEndpoint) GetType() task.ActionType {
	return task.ActionTypeEndpoint
}

func (a *ActionEcho) Execute(ctx context.Context, ec task.ExecutionContext) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionEcho) GetType() task.ActionType {
	return task.ActionTypeEcho
}

func (a *ActionDelay) Execute(ctx context.Context, ec task.ExecutionContext) error {
	logger.Infof("ActionDelay: sleeping for %s", a.Time)
	select {
	case <-time.After(a.Time):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *ActionDelay) GetType() task.ActionType {
	return task.ActionTypeDelay
}
