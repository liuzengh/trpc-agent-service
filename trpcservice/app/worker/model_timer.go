// model_timer.go implements a runner plugin that sums the model-call latency
// of one agent turn, so the platform can expose model_call_duration separate
// from the end-to-end agent_run_duration. A turn usually involves several
// model calls (multi-step reasoning); the plugin accumulates their durations.
package worker

import (
	"context"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
)

// modelTimer is a plugin.Plugin whose BeforeModel/AfterModel callbacks bracket
// every model invocation. The plugin is created per turn (each turn builds a
// fresh runner), so the accumulated total is turn-scoped. Model calls within
// one turn may overlap (parallel/cycle agents), so the timer tracks in-flight
// calls and measures the union of the bracketed windows — the total wall-clock
// during which at least one model call was running — rather than summing each
// call independently (which would double-count overlap).
type modelTimer struct {
	mu       sync.Mutex
	inflight int
	start    time.Time
	total    time.Duration
}

// newModelTimer returns an empty model timer.
func newModelTimer() *modelTimer { return &modelTimer{} }

// Name implements plugin.Plugin.
func (t *modelTimer) Name() string { return "platform-model-timer" }

// Register implements plugin.Plugin.
func (t *modelTimer) Register(r *plugin.Registry) {
	r.BeforeModel(t.beforeModel)
	r.AfterModel(t.afterModel)
}

func (t *modelTimer) beforeModel(_ context.Context, _ *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
	t.mu.Lock()
	if t.inflight == 0 {
		t.start = time.Now()
	}
	t.inflight++
	t.mu.Unlock()
	return nil, nil
}

func (t *modelTimer) afterModel(_ context.Context, _ *model.AfterModelArgs) (*model.AfterModelResult, error) {
	t.mu.Lock()
	if t.inflight > 0 {
		t.inflight--
		if t.inflight == 0 {
			t.total += time.Since(t.start)
			t.start = time.Time{}
		}
	}
	t.mu.Unlock()
	return nil, nil
}

// Duration returns the accumulated model-call latency since creation.
func (t *modelTimer) Duration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.total
}
