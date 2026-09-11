package domain

import "sort"

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

type Diagnostic struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	Pointer  string   `json:"pointer"`
	NodeID   *string  `json:"node_id"`
	Message  string   `json:"message"`
}

type ValidationReport struct {
	Valid         bool         `json:"valid"`
	SchemaVersion string       `json:"schema_version"`
	DraftRevision int64        `json:"draft_revision"`
	Diagnostics   []Diagnostic `json:"diagnostics"`
}

func newReport(schemaVersion string, revision int64, diagnostics []Diagnostic) ValidationReport {
	sort.SliceStable(diagnostics, func(i, j int) bool {
		if diagnostics[i].Pointer != diagnostics[j].Pointer {
			return diagnostics[i].Pointer < diagnostics[j].Pointer
		}
		if diagnostics[i].Severity != diagnostics[j].Severity {
			return diagnostics[i].Severity < diagnostics[j].Severity
		}
		return diagnostics[i].Code < diagnostics[j].Code
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

func errorDiagnostic(code, pointer, message string) Diagnostic {
	return Diagnostic{Code: code, Severity: SeverityError, Pointer: pointer, Message: message}
}

func nodeDiagnostic(code string, severity Severity, pointer, nodeID, message string) Diagnostic {
	id := nodeID
	return Diagnostic{Code: code, Severity: severity, Pointer: pointer, NodeID: &id, Message: message}
}
