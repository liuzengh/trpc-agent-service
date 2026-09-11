package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Metrics is a small dependency-free Prometheus exporter. Labels are limited
// to tenant/component/result; request, user, session and trace IDs are banned
// to keep cardinality bounded.
type Metrics struct {
	mu     sync.RWMutex
	values map[string]float64
	help   map[string]string
	types  map[string]string
}

func NewMetrics() *Metrics {
	return &Metrics{values: make(map[string]float64), help: make(map[string]string), types: make(map[string]string)}
}

func (m *Metrics) Add(name, help string, value float64, labels map[string]string) {
	key := metricKey(name, labels)
	m.mu.Lock()
	m.values[key] += value
	m.help[name] = help
	m.types[name] = "counter"
	m.mu.Unlock()
}

// Set records the current value of a gauge, replacing the previous sample for
// the same bounded label set.
func (m *Metrics) Set(name, help string, value float64, labels map[string]string) {
	key := metricKey(name, labels)
	m.mu.Lock()
	m.values[key] = value
	m.help[name] = help
	m.types[name] = "gauge"
	m.mu.Unlock()
}

// Observe records a cumulative Prometheus histogram in seconds. All samples
// are updated under one lock so a scrape never sees mismatched bucket/count.
func (m *Metrics) Observe(name, help string, value float64, labels map[string]string) {
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.help[name] = help
	m.types[name] = "histogram"
	m.values[metricKey(name+"_sum", labels)] += value
	m.values[metricKey(name+"_count", labels)]++
	for _, upper := range []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60} {
		key := bucketKey(name, labels, strconv.FormatFloat(upper, 'g', -1, 64))
		if _, ok := m.values[key]; !ok {
			m.values[key] = 0
		}
		if value <= upper {
			m.values[key]++
		}
	}
	m.values[bucketKey(name, labels, "+Inf")]++
}
func bucketKey(name string, labels map[string]string, upper string) string {
	key := metricKey(name+"_bucket", labels)
	if strings.HasSuffix(key, "}") {
		return strings.TrimSuffix(key, "}") + ",le=\"" + upper + "\"}"
	}
	return key + "{le=\"" + upper + "\"}"
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.help))
	for name := range m.help {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, m.help[name], name, m.types[name])
		keys := make([]string, 0)
		for key := range m.values {
			histogramSample := m.types[name] == "histogram" &&
				(key == name+"_sum" || key == name+"_count" || strings.HasPrefix(key, name+"_sum{") || strings.HasPrefix(key, name+"_count{") || strings.HasPrefix(key, name+"_bucket{"))
			if key == name || strings.HasPrefix(key, name+"{") || histogramSample {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			_, _ = io.WriteString(w, key+" "+strconv.FormatFloat(m.values[key], 'f', -1, 64)+"\n")
		}
	}
}

func metricKey(name string, labels map[string]string) string {
	allowed := []string{"tenant", "component", "result", "channel", "backend"}
	parts := make([]string, 0, len(allowed))
	for _, key := range allowed {
		if value, ok := labels[key]; ok && value != "" {
			parts = append(parts, key+"=\""+escapeLabel(value)+"\"")
		}
	}
	if len(parts) == 0 {
		return name
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}
