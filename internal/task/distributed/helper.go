package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func fetchConfigValue(ctx context.Context, member string, configMapName string, key string) (int, error) {
	const (
		maxRetries     = 3
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 4 * time.Second
	)

	client := httpclient.Get()
	url := fmt.Sprintf("%s/api/v1/configmaps/default/%s", member, configMapName)

	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			logger.Infof("fetchConfigValue: retrying attempt %d/%d for %s after %v", attempt, maxRetries, url, backoff)
			select {
			case <-ctx.Done():
				return 0, fmt.Errorf("context cancelled while retrying: %w", ctx.Err())
			case <-time.After(backoff):
			}
			// Exponential backoff with jitter
			backoff = time.Duration(float64(backoff) * 2.0)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request to %s: %w", member, err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to make request to %s: %w", member, err)
			logger.Warnf("fetchConfigValue: request failed (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		// Check for retryable HTTP status codes
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			resp.Body.Close()
			lastErr = fmt.Errorf("received retryable status code %d from %s", resp.StatusCode, member)
			logger.Warnf("fetchConfigValue: received status %d (attempt %d/%d)", resp.StatusCode, attempt+1, maxRetries+1)
			continue
		}

		// Non-retryable error status codes
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return 0, fmt.Errorf("received non-retryable status code %d from %s", resp.StatusCode, member)
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body from %s: %w", member, err)
			logger.Warnf("fetchConfigValue: failed to read response body (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		var configMap struct {
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(bodyBytes, &configMap); err != nil {
			lastErr = fmt.Errorf("failed to parse ConfigMap JSON from %s: %w", member, err)
			logger.Warnf("fetchConfigValue: failed to parse JSON (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		if key == "" {
			return 0, fmt.Errorf("key is required but not specified for ConfigMap %s from %s", configMapName, member)
		}

		valueStr, exists := configMap.Data[key]
		if !exists {
			return 0, nil
		}

		value, err := strconv.Atoi(strings.TrimSpace(valueStr))
		if err != nil {
			return 0, fmt.Errorf("failed to parse value '%s' as integer from %s: %w", valueStr, member, err)
		}

		if attempt > 0 {
			logger.Infof("fetchConfigValue: successfully fetched config value after %d retries from %s", attempt, member)
		}

		return value, nil
	}

	return 0, fmt.Errorf("failed to fetch config value after %d attempts: %w", maxRetries+1, lastErr)
}

func patchResource(ctx context.Context, member string, resource string, namespace string, name string, patchData map[string]interface{}, epoch int64, idempotencyId string) error {
	// Create patch payload
	patchPayload := map[string]interface{}{
		"epoch": epoch,
		"patch": patchData,
	}

	patchBody, err := json.Marshal(patchPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch payload: %w", err)
	}

	// Make PATCH request
	client := httpclient.Get()

	patchURL := fmt.Sprintf("%s/api/v1/%s/%s/%s", member, resource, namespace, name)
	req, err := http.NewRequestWithContext(ctx, "PATCH", patchURL, bytes.NewBuffer(patchBody))
	if err != nil {
		return fmt.Errorf("failed to create PATCH request to %s: %w", member, err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", idempotencyId)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute PATCH request to %s: %w", member, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func patchConfigValue(ctx context.Context, member string, configMapName string, key string, newValue int, epoch int64, idempotencyId string) error {
	patchData := map[string]interface{}{
		"data": map[string]string{
			key: strconv.Itoa(newValue),
		},
	}

	return patchResource(ctx, member, "configmaps", "default", configMapName, patchData, epoch, idempotencyId)
}
