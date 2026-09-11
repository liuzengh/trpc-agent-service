package telemetrytrace

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func testConfig(endpoint string) Config {
	return Config{TracesEndpoint: endpoint, SamplingRatio: 1, ExportTimeout: "200ms", BatchTimeout: "1h", MaxQueueSize: 64, MaxExportBatchSize: 16}
}
func TestClosedConfiguration(t *testing.T) {
	good := testConfig("http://127.0.0.1:4318/v1/traces")
	raw, _ := json.Marshal(good)
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"null", "{}", strings.Replace(string(raw), `"sampling_ratio":1`, `"sampling_ratio":null`, 1),
		strings.Replace(string(raw), `"sampling_ratio":1`, `"sampling_ratio":1,"sampling_ratio":0`, 1),
		strings.Replace(string(raw), `"sampling_ratio":1`, `"Sampling_Ratio":1`, 1),
		strings.TrimSuffix(string(raw), "}") + `,"unknown":true}`,
		strings.Replace(string(raw), `"export_timeout":"200ms"`, `"export_timeout":0`, 1),
	} {
		if json.Unmarshal([]byte(raw), &c) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	c = good
	c.SamplingRatio = 0
	zero, _ := json.Marshal(c)
	if json.Unmarshal(zero, &c) != nil {
		t.Fatal("explicit zero ratio rejected")
	}
	for _, endpoint := range []string{"https://user:secret@collector/v1/traces", "https://collector/v1/traces?q=secret", "https://collector/v1/traces#secret", "https://collector/v1/metrics", "http://collector/v1/traces", "http://localhost:4318/v1/traces", "https://collector/v1/%74races"} {
		c = good
		c.TracesEndpoint = endpoint
		if c.Validate() == nil {
			t.Fatalf("accepted endpoint %s", endpoint)
		}
	}
	for _, ratio := range []float64{-1, 2, math.NaN(), math.Inf(1)} {
		c = good
		c.SamplingRatio = ratio
		if c.Validate() == nil {
			t.Fatal(ratio)
		}
	}
	c = good
	c.MaxQueueSize = 1
	c.MaxExportBatchSize = 2
	if c.Validate() == nil {
		t.Fatal("invalid queue accepted")
	}
}
