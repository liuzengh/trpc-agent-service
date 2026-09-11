package agent

import "errors"

// permanentExecutionError marks failures that cannot change for the same
// immutable queued message/configuration. Kafka may dead-letter these without
// spending bounded retries on the same deterministic rejection. Unknown
// failures remain retryable so infrastructure/provider outages fail safe.
type permanentExecutionError struct{ error }

func (e permanentExecutionError) Unwrap() error { return e.error }

func markPermanentExecution(err error) error {
	if err == nil {
		return nil
	}
	return permanentExecutionError{error: err}
}

func isPermanentExecution(err error) bool {
	var target permanentExecutionError
	return errors.As(err, &target)
}
