package task

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (c *ConditionEndpointSuccess) Evaluate(ctx context.Context) (bool, error) {
	if c.Endpoint == "" {
		return false, fmt.Errorf("endpoint is required")
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != c.Status {
		return false, nil
	}

	// If ResponseBody is specified, check if it matches
	if c.ResponseBody != "" {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, fmt.Errorf("failed to read response body: %w", err)
		}
		bodyStr := strings.TrimSpace(string(bodyBytes))
		expectedBody := strings.TrimSpace(c.ResponseBody)
		if bodyStr != expectedBody {
			return false, nil
		}
	}

	return true, nil
}

func (c *ConditionEndpointValue) Evaluate(ctx context.Context) (bool, error) {
	if c.Endpoint == "" {
		return false, fmt.Errorf("endpoint is required")
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response body as integer
	bodyStr := strings.TrimSpace(string(bodyBytes))
	responseValue, err := strconv.Atoi(bodyStr)
	if err != nil {
		return false, fmt.Errorf("failed to parse response body as integer: %w", err)
	}

	// Compare using operator
	switch c.Operator {
	case "eq":
		return responseValue == c.Value, nil
	case "ne":
		return responseValue != c.Value, nil
	case "lt":
		return responseValue < c.Value, nil
	case "gt":
		return responseValue > c.Value, nil
	case "le":
		return responseValue <= c.Value, nil
	case "ge":
		return responseValue >= c.Value, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", c.Operator)
	}
}

func (c *ConditionAlwaysTrue) Evaluate(ctx context.Context) (bool, error) {
	return true, nil
}

func (c *ConditionEndpointSuccess) GetType() ConditionType {
	return ConditionTypeEndpointSuccess
}

func (c *ConditionEndpointValue) GetType() ConditionType {
	return ConditionTypeEndpointValue
}

func (c *ConditionAlwaysTrue) GetType() ConditionType {
	return ConditionTypeAlwaysTrue
}

func (a *ActionEndpoint) Execute(ctx context.Context, epoch int64) error {
	if a.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
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
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers if provided
	for key, value := range a.Headers {
		req.Header.Set(key, value)
	}

	// Set Content-Type header if body is provided and Content-Type is not already set
	if a.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Execute request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request to %s: %w", a.Endpoint, err)
	}
	defer resp.Body.Close()

	// Check for HTTP error status codes (4xx, 5xx)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("request failed with status code %d", resp.StatusCode)
	}

	return nil
}

func (a *ActionEcho) Execute(ctx context.Context, epoch int64) error {
	fmt.Println(a.Message)
	return nil
}

func (a *ActionEndpoint) GetType() ActionType {
	return ActionTypeEndpoint
}

func (a *ActionEcho) GetType() ActionType {
	return ActionTypeEcho
}
