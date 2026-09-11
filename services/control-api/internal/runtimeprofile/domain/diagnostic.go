package domain

import "sort"

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Diagnostic is a stable, non-secret description of a document problem.
// ResourceKind and ResourceKey are intentionally required nullable fields in
// JSON so clients never have to distinguish missing from null.
type Diagnostic struct {
	Code         string   `json:"code"`
	Severity     Severity `json:"severity"`
	Pointer      string   `json:"pointer"`
	ResourceKind *string  `json:"resource_kind"`
	ResourceKey  *string  `json:"resource_key"`
	Message      string   `json:"message"`
}

type ValidationReport struct {
	Valid         bool         `json:"valid"`
	SchemaVersion string       `json:"schema_version"`
	DraftRevision int64        `json:"draft_revision"`
	Diagnostics   []Diagnostic `json:"diagnostics"`
}

func newReport(schemaVersion string, revision int64, diagnostics []Diagnostic) ValidationReport {
	sort.SliceStable(diagnostics, func(i, j int) bool {
		left, right := diagnostics[i], diagnostics[j]
		if left.Pointer != right.Pointer {
			return left.Pointer < right.Pointer
		}
		if left.Severity != right.Severity {
			return left.Severity < right.Severity
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if pointerValue(left.ResourceKind) != pointerValue(right.ResourceKind) {
			return pointerValue(left.ResourceKind) < pointerValue(right.ResourceKind)
		}
		return pointerValue(left.ResourceKey) < pointerValue(right.ResourceKey)
	})
	valid := true
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == SeverityError {
			valid = false
			break
		}
	}
	if diagnostics == nil {
		diagnostics = []Diagnostic{}
	}
	return ValidationReport{
		Valid: valid, SchemaVersion: schemaVersion,
		DraftRevision: revision, Diagnostics: diagnostics,
	}
}

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func errorDiagnostic(code, pointer, message string) Diagnostic {
	return Diagnostic{Code: code, Severity: SeverityError, Pointer: pointer, Message: message}
}

func resourceDiagnostic(
	code string,
	severity Severity,
	pointer, resourceKind, resourceKey, message string,
) Diagnostic {
	kind, key := resourceKind, resourceKey
	return Diagnostic{
		Code: code, Severity: severity, Pointer: pointer,
		ResourceKind: &kind, ResourceKey: &key, Message: message,
	}
}
