package configpub

import "errors"

// classifiedError marks a failure whose raw cause must never surface upward.
// It matches ErrUnavailable via errors.Is, keeping a bounded identity only.
type classifiedError struct {
	boundary string
	cause    error
}

func (e *classifiedError) Error() string { return "configpub: " + e.boundary + " unavailable" }
func (e *classifiedError) Unwrap() error { return e.cause }

// Is makes every classified failure match ErrUnavailable for errors.Is chains.
func (e *classifiedError) Is(target error) bool { return target == ErrUnavailable }

// classify maps a raw backend failure to the safe unavailable class. Unknown
// errors fail closed instead of leaking backend detail.
func classify(boundary string, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{boundary: boundary, cause: err}
}

// isUnavailable reports whether the error class means "the configuration fact
// source could not be reached or answered". Such failures fail closed.
func isUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}
