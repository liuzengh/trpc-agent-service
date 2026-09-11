// Package telemetrytrace owns the shared technical trace pipeline. It has no
// domain state, database access, credentials, or SDK Agent dependency.
package telemetrytrace

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
)

var ErrConfig = errors.New("invalid explicit tracing configuration")

type Config struct {
	TracesEndpoint     string  `json:"traces_endpoint"`
	SamplingRatio      float64 `json:"sampling_ratio"`
	ExportTimeout      string  `json:"export_timeout"`
	BatchTimeout       string  `json:"batch_timeout"`
	MaxQueueSize       int     `json:"max_queue_size"`
	MaxExportBatchSize int     `json:"max_export_batch_size"`
}

// UnmarshalJSON enforces the same closed object for Gateway files and Worker
// configuration, including required zero-valued ratio, duplicate keys and nulls.
func (c *Config) UnmarshalJSON(raw []byte) error {
	if len(raw) > 64*1024 {
		return ErrConfig
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return ErrConfig
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(canonical, &fields) != nil || len(fields) != 6 {
		return ErrConfig
	}
	for _, key := range []string{"traces_endpoint", "sampling_ratio", "export_timeout", "batch_timeout", "max_queue_size", "max_export_batch_size"} {
		v, ok := fields[key]
		if !ok || bytes.Equal(v, []byte("null")) {
			return ErrConfig
		}
	}
	type plain Config
	var next plain
	d := json.NewDecoder(bytes.NewReader(canonical))
	d.DisallowUnknownFields()
	if d.Decode(&next) != nil || d.Decode(new(any)) != io.EOF {
		return ErrConfig
	}
	value := Config(next)
	if value.Validate() != nil {
		return ErrConfig
	}
	*c = value
	return nil
}

func (c Config) Validate() error {
	u, err := url.Parse(c.TracesEndpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery ||
		strings.Contains(c.TracesEndpoint, "#") || u.RawPath != "" || u.Opaque != "" || u.Path != "/v1/traces" ||
		(u.Scheme != "https" && u.Scheme != "http") {
		return ErrConfig
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return ErrConfig
		}
	}
	if math.IsNaN(c.SamplingRatio) || math.IsInf(c.SamplingRatio, 0) ||
		c.SamplingRatio < 0 || c.SamplingRatio > 1 || c.MaxQueueSize < 1 ||
		c.MaxExportBatchSize < 1 || c.MaxExportBatchSize > c.MaxQueueSize {
		return ErrConfig
	}
	for _, v := range []string{c.ExportTimeout, c.BatchTimeout} {
		d, e := time.ParseDuration(v)
		if e != nil || d < time.Millisecond {
			return ErrConfig
		}
	}
	return nil
}
