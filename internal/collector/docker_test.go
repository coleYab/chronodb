package collector

import (
	"os"
	"testing"
)

func TestDocker_UnixSocketPath(t *testing.T) {
	sockPath := "/var/run/docker.sock"
	if _, err := os.Stat(sockPath); err != nil {
		t.Skip("Docker socket not available")
	}
	// Just verify the socket exists and is accessible
	t.Logf("Docker socket found at %s", sockPath)
}

func TestDocker_HTTPClientForUnix(t *testing.T) {
	client := httpClientForUnix("unix:///var/run/docker.sock")
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Timeout == 0 {
		t.Fatal("expected non-zero timeout")
	}
	// Don't actually make a request since socket may not exist
	client.CloseIdleConnections()
}

func TestDocker_NewCollectorDefaults(t *testing.T) {
	dc := NewDockerCollector(DockerCollectorConfig{
		Interval:    0,
		Endpoint:    "",
		DefaultTags: map[string]string{"host": "test"},
	})
	if dc == nil {
		t.Fatal("expected non-nil collector")
	}
	// interval defaults to 15s when Run() is called; zero is valid pre-Run
	_ = dc.interval
	if dc.client == nil {
		t.Fatal("expected non-nil HTTP client")
	}
	dc.client.CloseIdleConnections()
}
