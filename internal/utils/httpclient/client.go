package httpclient

import (
	"net/http"
	"sync"
	"time"
)

// DefaultTimeout is the default timeout for HTTP requests.
const DefaultTimeout = 30 * time.Second

var (
	defaultClient    *http.Client
	once             sync.Once
	longRunnerClient *http.Client
	onceLongRunner   sync.Once
)

// Get returns the default HTTP client.
// This client should be reused across the application to avoid
// creating new TCP connections for each request.
// The client is created lazily using sync.Once for thread-safety.
func Get() *http.Client {
	once.Do(func() {
		defaultClient = &http.Client{
			Timeout: DefaultTimeout,
		}
	})
	return defaultClient
}

// GetLongRunner returns an HTTP client without a client-level timeout.
// Use this for long-running operations where the timeout should be
// controlled by the request context instead of the client.
// This allows operations like waiting for deployment rollouts to use
// custom timeouts via context.WithTimeout.
func GetLongRunner() *http.Client {
	onceLongRunner.Do(func() {
		longRunnerClient = &http.Client{
			// No Timeout set - rely on context for timeout control
		}
	})
	return longRunnerClient
}
