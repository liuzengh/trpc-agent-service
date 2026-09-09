// Package log configures log levels and redaction for secrets.
package log

import (
	"io"
	"net/url"
	"sort"
	"strings"
)

type Redactor struct{ values []string }

type RedactingWriter struct {
	destination io.Writer
	redactor    Redactor
}

func NewRedactingWriter(destination io.Writer, redactor Redactor) io.Writer {
	return &RedactingWriter{destination: destination, redactor: redactor}
}

func (w *RedactingWriter) Write(data []byte) (int, error) {
	redacted := []byte(w.redactor.Redact(string(data)))
	if _, err := w.destination.Write(redacted); err != nil {
		return 0, err
	}
	return len(data), nil
}

func NewRedactor(secrets, patterns []string) Redactor {
	seen := map[string]bool{}
	values := []string{}
	for _, value := range append(append([]string{}, secrets...), patterns...) {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			values = append(values, value)
		}
		if parsed, err := url.Parse(value); err == nil && parsed.User != nil {
			if password, ok := parsed.User.Password(); ok && password != "" && !seen[password] {
				seen[password] = true
				values = append(values, password)
			}
		}
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return Redactor{values: values}
}

func (r Redactor) Redact(value string) string {
	for _, sensitive := range r.values {
		value = strings.ReplaceAll(value, sensitive, "[REDACTED]")
	}
	return value
}
