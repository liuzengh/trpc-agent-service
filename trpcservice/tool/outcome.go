package tool

import "errors"

// Outcome is what a tool call did, in the three states the recovery protocol
// and the journal need to distinguish (approved plan, "受控工具" and
// "不确定结果人工核对"):
//
//   - Succeeded: the call returned a result.
//   - Failed: it did not, and nothing could have happened outside this
//     process — a rejected request, a 4xx, a connection that was never
//     established. Safe to report to the model and safe to re-run.
//   - Unknown: the call may or may not have arrived. For a side-effecting
//     call this is the "外部副作用不确定" state: the execution blocks and a
//     human decides, because an automatic retry is exactly how an effect
//     runs twice.
type Outcome int

const (
	Succeeded Outcome = iota
	Failed
	Unknown
)

func (o Outcome) String() string {
	switch o {
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Unknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// CallError is how a callable tells the governor which of the three outcomes
// it produced. A plain error from a callable is treated as Failed: a callable
// that cannot classify its own failure says "nothing happened", and only the
// HTTP tool — which can see whether a request went out — ever claims Unknown.
type CallError struct {
	Outcome   Outcome
	ErrorType string
	Err       error

	// retryableStatus is HTTP-internal: whether the status this error carries
	// is worth one more attempt under the policy. Unexported because it is
	// the retry loop's own state, not part of what a caller classifies.
	retryableStatus bool
}

func (e *CallError) Error() string {
	if e.Err == nil {
		return "tool: call " + e.Outcome.String()
	}
	return e.Err.Error()
}

func (e *CallError) Unwrap() error { return e.Err }

// classify resolves any error a callable returned into (outcome, type,
// message). It is the one place the governor decides, so journal rows and
// audit rows agree about what happened.
func classify(err error) (Outcome, string, string) {
	if err == nil {
		return Succeeded, "", ""
	}
	var ce *CallError
	if errors.As(err, &ce) {
		return ce.Outcome, ce.ErrorType, ce.Error()
	}
	return Failed, "tool_error", err.Error()
}
