package tenant

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// CanaryAction is the control-plane action selected after evaluating candidate
// health. None means that thresholds were not breached.
type CanaryAction string

const (
	CanaryActionNone     CanaryAction = "NONE"
	CanaryActionPause    CanaryAction = "PAUSE"
	CanaryActionRollback CanaryAction = "ROLLBACK"
)

// CanaryObservations contains one already-aggregated candidate metric window.
// Metric collection and PromQL remain outside the control plane; this type is
// the small, validated hand-off between them.
type CanaryObservations struct {
	Samples          int64         `json:"samples"`
	ErrorRate        float64       `json:"error_rate"`
	P95Latency       time.Duration `json:"p95_latency"`
	BudgetRejections int64         `json:"budget_rejections"`
}

// CanaryRule defines minimum evidence and optional upper bounds. A nil bound
// disables that signal; zero is therefore a valid strict threshold.
type CanaryRule struct {
	MinimumSamples      int64          `json:"minimum_samples"`
	MaxErrorRate        *float64       `json:"max_error_rate,omitempty"`
	MaxP95Latency       *time.Duration `json:"max_p95_latency,omitempty"`
	MaxBudgetRejections *int64         `json:"max_budget_rejections,omitempty"`
	Action              CanaryAction   `json:"action"`
}

// CanaryDecision is deterministic and contains only stable reason names so it
// can be safely written to control-plane audit metadata.
type CanaryDecision struct {
	Action  CanaryAction `json:"action"`
	Reasons []string     `json:"reasons,omitempty"`
}

// ErrCanaryDecisionStale means an automatic decision targeted an old
// candidate. Callers should treat it as a conflict and re-read rollout state.
var ErrCanaryDecisionStale = errors.New("canary decision target changed")

func (o CanaryObservations) Validate() error {
	if o.Samples < 0 {
		return errors.New("canary samples must be non-negative")
	}
	if math.IsNaN(o.ErrorRate) || math.IsInf(o.ErrorRate, 0) || o.ErrorRate < 0 || o.ErrorRate > 1 {
		return errors.New("canary error rate must be between 0 and 1")
	}
	if o.P95Latency < 0 {
		return errors.New("canary p95 latency must be non-negative")
	}
	if o.BudgetRejections < 0 {
		return errors.New("canary budget rejections must be non-negative")
	}
	return nil
}

func (r CanaryRule) Validate() error {
	if r.MinimumSamples <= 0 {
		return errors.New("canary minimum samples must be positive")
	}
	switch r.Action {
	case CanaryActionPause, CanaryActionRollback:
	default:
		return errors.New("canary action must be PAUSE or ROLLBACK")
	}
	if r.MaxErrorRate != nil && (math.IsNaN(*r.MaxErrorRate) || math.IsInf(*r.MaxErrorRate, 0) || *r.MaxErrorRate < 0 || *r.MaxErrorRate > 1) {
		return errors.New("canary max error rate must be between 0 and 1")
	}
	if r.MaxP95Latency != nil && *r.MaxP95Latency < 0 {
		return errors.New("canary max p95 latency must be non-negative")
	}
	if r.MaxBudgetRejections != nil && *r.MaxBudgetRejections < 0 {
		return errors.New("canary max budget rejections must be non-negative")
	}
	if r.MaxErrorRate == nil && r.MaxP95Latency == nil && r.MaxBudgetRejections == nil {
		return errors.New("canary rule must configure at least one threshold")
	}
	return nil
}

// Evaluate applies thresholds only after the minimum sample guard. It never
// changes rollout state; ApplyCanaryDecision owns the atomic state transition.
func (r CanaryRule) Evaluate(observations CanaryObservations) (CanaryDecision, error) {
	if err := observations.Validate(); err != nil {
		return CanaryDecision{}, err
	}
	if err := r.Validate(); err != nil {
		return CanaryDecision{}, err
	}
	if observations.Samples < r.MinimumSamples {
		return CanaryDecision{Action: CanaryActionNone}, nil
	}
	reasons := make([]string, 0, 3)
	if r.MaxErrorRate != nil && observations.ErrorRate > *r.MaxErrorRate {
		reasons = append(reasons, "error_rate")
	}
	if r.MaxP95Latency != nil && observations.P95Latency > *r.MaxP95Latency {
		reasons = append(reasons, "p95_latency")
	}
	if r.MaxBudgetRejections != nil && observations.BudgetRejections > *r.MaxBudgetRejections {
		reasons = append(reasons, "budget_rejections")
	}
	if len(reasons) == 0 {
		return CanaryDecision{Action: CanaryActionNone}, nil
	}
	return CanaryDecision{Action: r.Action, Reasons: reasons}, nil
}

// AuditReason returns a bounded, stable reason string. Values stay out of the
// audit reason because the event schema is metadata-only and already enforces
// a safe policy-reason alphabet.
func (d CanaryDecision) AuditReason() string {
	if len(d.Reasons) == 0 {
		return ""
	}
	return strings.Join(d.Reasons, "_")
}

func (a CanaryAction) Validate() error {
	if a != CanaryActionNone && a != CanaryActionPause && a != CanaryActionRollback {
		return fmt.Errorf("canary action %q is invalid", a)
	}
	return nil
}
