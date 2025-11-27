package httpclient

import (
	"net/http"
	"time"
)

// DefaultTimeout is the default timeout for HTTP requests.
const DefaultTimeout = 30 * time.Second

// defaultClient is a reusable HTTP client with default settings.
var defaultClient = &http.Client{
	Timeout: DefaultTimeout,
}

// Get returns the default HTTP client.
// This client should be reused across the application to avoid
// creating new TCP connections for each request.
func Get() *http.Client {
	return defaultClient
}
