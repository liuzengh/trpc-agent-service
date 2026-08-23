// Package secrets resolves secret references without exposing secret material in config.
package secrets

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Provider interface {
	Resolve(context.Context, string) ([]string, error)
}

// FileEnvProvider supports env:NAME and fileenv:NAME references. fileenv reads
// the path from NAME and returns non-empty lines, accepting optional key:value.
type FileEnvProvider struct{}

func (FileEnvProvider) Resolve(_ context.Context, ref string) ([]string, error) {
	kind, name, ok := strings.Cut(ref, ":")
	if !ok || name == "" {
		return nil, errors.New("secret reference must use env:NAME or fileenv:NAME")
	}
	switch kind {
	case "env":
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return nil, fmt.Errorf("secret environment variable %s is empty", name)
		}
		return []string{value}, nil
	case "fileenv":
		path := strings.TrimSpace(os.Getenv(name))
		if path == "" {
			return nil, fmt.Errorf("credential path environment variable %s is empty", name)
		}
		return readCredentialFile(path)
	default:
		return nil, fmt.Errorf("unsupported secret reference kind %q", kind)
	}
}

func readCredentialFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open credential file: %w", err)
	}
	defer f.Close()
	var values []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if key, value, ok := cutLabeled(line); ok {
			_ = key
			line = value
		}
		if line != "" {
			values = append(values, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read credential file: %w", err)
	}
	if len(values) == 0 {
		return nil, errors.New("credential file contains no values")
	}
	return values, nil
}

func cutLabeled(line string) (string, string, bool) {
	colon := strings.Index(line, ":")
	equal := strings.Index(line, "=")
	idx := colon
	if idx < 0 || equal >= 0 && equal < idx {
		idx = equal
	}
	if idx <= 0 {
		return "", "", false
	}
	key := strings.TrimSpace(line[:idx])
	value := strings.TrimSpace(line[idx+1:])
	if key == "" || value == "" {
		return "", "", false
	}
	return key, value, true
}

func Fingerprint(values []string) string {
	if len(values) == 0 || len(values[len(values)-1]) < 4 {
		return "****"
	}
	last := values[len(values)-1]
	return "****" + last[len(last)-4:]
}
