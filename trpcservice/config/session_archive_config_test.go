package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadSessionIdleArchiveAge(t *testing.T) {
	loaded, err := Load(strings.NewReader(`{
		"service":{"listen_address":":8080","request_timeout":"1s","max_inbound_bytes":1,"session_idle_archive_age":"12h"}
	}`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Service.SessionIdleArchiveAge.Duration != 12*time.Hour {
		t.Fatalf("session idle archive age = %v, want 12h", loaded.Service.SessionIdleArchiveAge.Duration)
	}
	_, err = Load(strings.NewReader(`{
		"service":{"listen_address":":8080","request_timeout":"1s","max_inbound_bytes":1,"session_idle_archive_age":"-1h"}
	}`))
	if err == nil || !strings.Contains(err.Error(), "session_idle_archive_age") {
		t.Fatalf("negative archive age error = %v, want validation failure", err)
	}
}
