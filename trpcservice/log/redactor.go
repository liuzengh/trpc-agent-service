package log

import "github.com/liuzengh/trpc-agent-service/trpcservice/redaction"

// Redactor is re-exported from the logging package so existing logging
// integrations can use the same process-wide rules as HTTP, trace and audit.
type Redactor = redaction.Redactor

func NewRedactor(extra []string) (Redactor, error) { return redaction.New(extra) }
func DefaultRedactor() Redactor                    { return redaction.Default }
