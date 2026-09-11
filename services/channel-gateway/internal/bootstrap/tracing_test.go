package bootstrap

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGatewayTracingFileExplicitAndClosed(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://unexpected")
	c, err := loadTracingConfig("")
	if err != nil || c != nil {
		t.Fatal("implicit exporter")
	}
	path := filepath.Join(t.TempDir(), "tracing.json")
	good := `{"traces_endpoint":"http://127.0.0.1:4318/v1/traces","sampling_ratio":1,"export_timeout":"1s","batch_timeout":"1s","max_queue_size":32,"max_export_batch_size":8}`
	if err = os.WriteFile(path, []byte(good), 0600); err != nil {
		t.Fatal(err)
	}
	c, err = loadTracingConfig(path)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	for _, body := range []string{"null", "{}", good + " {}", `{"tracing":` + good + "}"} {
		os.WriteFile(path, []byte(body), 0600)
		if _, err = loadTracingConfig(path); err == nil {
			t.Fatal("invalid file accepted")
		}
	}
	if _, err = loadTracingConfig("relative.json"); err == nil {
		t.Fatal("relative config accepted")
	}
}
