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
