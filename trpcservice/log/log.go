// Package log configures structured-safe redaction for process logs.
package log

import (
	"io"
	"regexp"
	"strings"
)

var redactors = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+`),
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/-]{8,}`),
	regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token|password|secret)(["']?\s*[:=]\s*["']?)[^\s,"'}]+`),
	regexp.MustCompile(`(postgres(?:ql)?|redis|rediss)://([^:/@\s]+):([^@\s]+)@`),
	regexp.MustCompile(`(?i)(https?://[^/\s]+/bot)[^/\s?"']+`),
}

func Redact(value string) string {
	result := value
	result = redactors[0].ReplaceAllString(result, `${1}[REDACTED]`)
	result = redactors[1].ReplaceAllString(result, `${1}[REDACTED]`)
	result = redactors[2].ReplaceAllString(result, `${1}${2}[REDACTED]`)
	result = redactors[3].ReplaceAllString(result, `${1}://[REDACTED]@`)
	result = redactors[4].ReplaceAllString(result, `${1}[REDACTED]`)
	return result
}

type redactingWriter struct{ target io.Writer }

func NewRedactingWriter(target io.Writer) io.Writer {
	if target == nil {
		target = io.Discard
	}
	return &redactingWriter{target: target}
}

func (w *redactingWriter) Write(value []byte) (int, error) {
	redacted := Redact(string(value))
	_, err := io.Copy(w.target, strings.NewReader(redacted))
	if err != nil {
		return 0, err
	}
	return len(value), nil
}
