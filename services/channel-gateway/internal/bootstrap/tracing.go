package bootstrap

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
)

// The optional Gateway file contains the same six-field object as Worker's
// root tracing property. Missing path disables tracing; no OTEL env fallback.
func loadTracingConfig(path string) (*telemetrytrace.Config, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("Gateway tracing requires an absolute config path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
		return nil, errors.New("Gateway tracing config unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("Gateway tracing config unavailable")
	}
	defer clear(raw)
	var c telemetrytrace.Config
	if json.Unmarshal(raw, &c) != nil {
		return nil, telemetrytrace.ErrConfig
	}
	return &c, nil
}
