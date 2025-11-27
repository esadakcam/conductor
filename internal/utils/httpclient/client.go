package httpclient

import (
	"net/http"
	"sync"
	"time"
)

// DefaultTimeout is the default timeout for HTTP requests.
const DefaultTimeout = 30 * time.Second

var (
	defaultClient *http.Client
	once          sync.Once
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
