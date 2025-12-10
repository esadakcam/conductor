package task

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func (c *ConditionEndpointSuccess) Evaluate(ctx context.Context) (bool, error) {
	if c.Endpoint == "" {
		err := fmt.Errorf("endpoint is required")
		logger.Error("ConditionEndpointSuccess: endpoint is required")
		return false, err
	}

	client := httpclient.Get()

	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		logger.Errorf("ConditionEndpointSuccess: failed to create request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionEndpointSuccess: failed to make request to %s: %v", c.Endpoint, err)
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
			logger.Errorf("ConditionEndpointSuccess: failed to read response body from %s: %v", c.Endpoint, err)
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
		err := fmt.Errorf("endpoint is required")
		logger.Error("ConditionEndpointValue: endpoint is required")
		return false, err
	}

	client := httpclient.Get()

	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to create request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to make request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to read response body from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response body as integer
	bodyStr := strings.TrimSpace(string(bodyBytes))
	responseValue, err := strconv.Atoi(bodyStr)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to parse response body as integer from %s: %v", c.Endpoint, err)
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
		err := fmt.Errorf("unsupported operator: %s", c.Operator)
		logger.Errorf("ConditionEndpointValue: %v", err)
		return false, err
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
