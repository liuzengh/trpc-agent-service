// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrToolNotVisible reports that a tool cannot be exposed to a tenant app.
var ErrToolNotVisible = errors.New("tool is not visible")

// ErrToolNotExecutable reports that a tool cannot be executed by a tenant app.
var ErrToolNotExecutable = errors.New("tool is not executable")

// Safety describes the external side-effect contract of a tool.
//
// The platform uses this metadata only for crash/retry decisions. It does not
// replace the tenant visibility, execution, or approval policy.
type Safety string

const (
	SafetyReadOnly   Safety = "read_only"
	SafetyIdempotent Safety = "idempotent"
	SafetySideEffect Safety = "side_effecting"
)

// FailureClass is the failure contract between a tool provider and Worker.
type FailureClass string

const (
	FailureInfrastructureRetryable   FailureClass = "infrastructure_retryable"
	FailurePermanent                 FailureClass = "permanent"
	FailureSideEffectResultKnown     FailureClass = "side_effect_result_known"
	FailureSideEffectResultUncertain FailureClass = "side_effect_result_uncertain"
)

// Validate checks that a safety class is explicit and supported.
func (s Safety) Validate() error {
	switch s {
	case SafetyReadOnly, SafetyIdempotent, SafetySideEffect:
		return nil
	default:
		return fmt.Errorf("unsupported tool safety %q", s)
	}
}

// Validate checks that a failure class is explicit and supported.
func (c FailureClass) Validate() error {
	switch c {
	case FailureInfrastructureRetryable, FailurePermanent,
		FailureSideEffectResultKnown, FailureSideEffectResultUncertain:
		return nil
	default:
		return fmt.Errorf("unsupported tool failure class %q", c)
	}
}

type classifiedFailure struct {
	class FailureClass
	err   error
}

func (e classifiedFailure) Error() string {
	if e.err == nil {
		return string(e.class)
	}
	return e.err.Error()
}

func (e classifiedFailure) Unwrap() error { return e.err }

func (e classifiedFailure) ToolFailureClass() FailureClass { return e.class }

// NewClassifiedFailure annotates a provider error without exposing its raw
// arguments or response. The annotation is consumed by the runtime retry
// boundary and remains available through errors.Is/errors.As.
func NewClassifiedFailure(class FailureClass, err error) error {
	if err == nil {
		return nil
	}
	if validationErr := class.Validate(); validationErr != nil {
		return fmt.Errorf("classify tool failure: %w", validationErr)
	}
	return classifiedFailure{class: class, err: err}
}

// NewInfrastructureRetryableFailure marks a failure safe to retry before the
// provider has performed an unknown side effect.
func NewInfrastructureRetryableFailure(err error) error {
	return NewClassifiedFailure(FailureInfrastructureRetryable, err)
}

// NewPermanentFailure marks a failure that must not cause an automatic retry.
func NewPermanentFailure(err error) error {
	return NewClassifiedFailure(FailurePermanent, err)
}

// NewSideEffectResultKnownFailure marks a side-effect failure whose external
// result is already known. The whole Tool must not be run again.
func NewSideEffectResultKnownFailure(err error) error {
	return NewClassifiedFailure(FailureSideEffectResultKnown, err)
}

// NewSideEffectResultUncertainFailure marks a possible external side effect
// whose result was not confirmed. The whole Tool must not be run again.
func NewSideEffectResultUncertainFailure(err error) error {
	return NewClassifiedFailure(FailureSideEffectResultUncertain, err)
}

// FailureClassOf returns an explicit provider classification, if present.
func FailureClassOf(err error) FailureClass {
	if err == nil {
		return ""
	}
	var classified interface{ ToolFailureClass() FailureClass }
	if errors.As(err, &classified) {
		return classified.ToolFailureClass()
	}
	return ""
}

// ClassifyFailure applies the fail-closed default for a Tool error. A
// side-effecting Tool with no explicit result classification is uncertain;
// read-only and idempotent tools are permanent unless their provider marks
// the error infrastructure-retryable.
func ClassifyFailure(safety Safety, err error) FailureClass {
	if err == nil {
		return ""
	}
	if class := FailureClassOf(err); class != "" {
		return class
	}
	if safety == SafetySideEffect {
		return FailureSideEffectResultUncertain
	}
	return FailurePermanent
}

type idempotencyKeyContextKey struct{}

// StableIdempotencyKey derives a deterministic key for a platform-controlled
// tool side effect. The raw argument bytes are never part of logs or storage;
// only their digest participates in the key.
func StableIdempotencyKey(tenantID, appID, requestID, toolCallID, toolName string, args []byte) string {
	argsDigest := sha256.Sum256(args)
	h := sha256.New()
	for _, value := range []string{tenantID, appID, requestID, toolCallID, toolName, hex.EncodeToString(argsDigest[:])} {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// WithIdempotencyKey makes a stable side-effect key available to a tool via
// its execution context.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, idempotencyKeyContextKey{}, key)
}

// IdempotencyKey returns the platform key attached to the current tool call.
func IdempotencyKey(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	key, _ := ctx.Value(idempotencyKeyContextKey{}).(string)
	return key
}

// AuthorizeVisibility checks the tenant tool policy before exposing a tool.
func AuthorizeVisibility(policy tenant.ToolPolicy, name string) error {
	if name == "" {
		return errors.New("tool name is required")
	}
	if !policy.CanView(name) {
		return fmt.Errorf("%w: %s", ErrToolNotVisible, name)
	}
	return nil
}

// AuthorizeExecution checks the tenant tool policy immediately before a tool run.
func AuthorizeExecution(policy tenant.ToolPolicy, name string) error {
	if name == "" {
		return errors.New("tool name is required")
	}
	if !policy.CanExecute(name) {
		return fmt.Errorf("%w: %s", ErrToolNotExecutable, name)
	}
	return nil
}
