// Package secret resolves credential references without exposing values to
// control-plane records, logs or traces.
package secret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

var ErrNotFound = errors.New("secret not found")

type Store interface {
	Resolve(ctx context.Context, reference string) (string, error)
}

// EnvStore resolves env://VARIABLE references for local development.
type EnvStore struct{}

func (EnvStore) Resolve(ctx context.Context, reference string) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	const prefix = "env://"
	if !strings.HasPrefix(reference, prefix) {
		return "", fmt.Errorf("unsupported secret reference: %w", ErrNotFound)
	}
	name := strings.TrimSpace(strings.TrimPrefix(reference, prefix))
	if name == "" {
		return "", fmt.Errorf("empty secret environment name")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("secret reference %q: %w", reference, ErrNotFound)
	}
	return value, nil
}

// StaticStore is used by tests and controlled local fixtures.
type StaticStore map[string]string

func (s StaticStore) Resolve(ctx context.Context, reference string) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	value, ok := s[reference]
	if !ok {
		return "", fmt.Errorf("secret reference %q: %w", reference, ErrNotFound)
	}
	return value, nil
}
