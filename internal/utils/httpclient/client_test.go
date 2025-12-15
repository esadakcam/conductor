package httpclient

import (
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	client := Get()
	if client == nil {
		t.Fatal("Get() returned nil")
	}
	if client.Timeout != DefaultTimeout {
		t.Errorf("Expected timeout %v, got %v", DefaultTimeout, client.Timeout)
	}
}

func TestDefaultTimeout(t *testing.T) {
	expected := 30 * time.Second
	if DefaultTimeout != expected {
		t.Errorf("Expected DefaultTimeout to be %v, got %v", expected, DefaultTimeout)
	}
}

func TestGetReturnsSameClient(t *testing.T) {
	client1 := Get()
	client2 := Get()
	if client1 != client2 {
		t.Error("Expected Get() to return the same client instance")
	}
}

func TestGetLongRunner(t *testing.T) {
	client := GetLongRunner()
	if client == nil {
		t.Fatal("GetLongRunner() returned nil")
	}
	// Long runner client should have no timeout (0 means no timeout)
	if client.Timeout != 0 {
		t.Errorf("Expected timeout 0 (no timeout), got %v", client.Timeout)
	}
}

func TestGetLongRunnerReturnsSameClient(t *testing.T) {
	client1 := GetLongRunner()
	client2 := GetLongRunner()
	if client1 != client2 {
		t.Error("Expected GetLongRunner() to return the same client instance")
	}
}

func TestGetAndGetLongRunnerAreDifferentClients(t *testing.T) {
	defaultClient := Get()
	longRunnerClient := GetLongRunner()
	if defaultClient == longRunnerClient {
		t.Error("Expected Get() and GetLongRunner() to return different client instances")
	}
}
