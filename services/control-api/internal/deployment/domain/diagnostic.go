package domain

import "sort"

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

type DiagnosticSource string

const (
	DiagnosticSourceInput    DiagnosticSource = "input"
	DiagnosticSourceAgent    DiagnosticSource = "agent"
	DiagnosticSourceProfile  DiagnosticSource = "profile"
	DiagnosticSourcePlatform DiagnosticSource = "platform"
)

const (
	DiagnosticInputInvalid            = "DEPLOYMENT_INPUT_INVALID"
	DiagnosticSourceSchemaUnsupported = "DEPLOYMENT_SOURCE_SCHEMA_UNSUPPORTED"
	DiagnosticResourceMissing         = "DEPLOYMENT_RESOURCE_MISSING"
	DiagnosticCapabilityMismatch      = "DEPLOYMENT_CAPABILITY_MISMATCH"
	DiagnosticUnusedRequirement       = "DEPLOYMENT_UNUSED_REQUIREMENT"
	DiagnosticStorageRoleMissing      = "DEPLOYMENT_STORAGE_ROLE_MISSING"
	DiagnosticStorageRoleUnsupported  = "DEPLOYMENT_STORAGE_ROLE_UNSUPPORTED"
	DiagnosticAdapterUnsupported      = "DEPLOYMENT_ADAPTER_UNSUPPORTED"
	DiagnosticEntrypointUnsupported   = "DEPLOYMENT_ENTRYPOINT_UNSUPPORTED"
	DiagnosticExecutionRangeDenied    = "DEPLOYMENT_EXECUTION_RANGE_DENIED"
	DiagnosticLimitExceeded           = "DEPLOYMENT_LIMIT_EXCEEDED"
	DiagnosticCredentialUnavailable   = "DEPLOYMENT_CREDENTIAL_UNAVAILABLE"
	DiagnosticManifestTooLarge        = "DEPLOYMENT_MANIFEST_TOO_LARGE"
)

// Diagnostic contains stable machine fields. Message is explanatory only and
// is deliberately last in deterministic sorting.
type Diagnostic struct {
	Code     string           `json:"code"`
	Severity Severity         `json:"severity"`
	Source   DiagnosticSource `json:"source"`
	Path     string           `json:"path"`
	Category *string          `json:"category"`
	Name     *string          `json:"name"`
	NodeID   *string          `json:"node_id"`
	Message  string           `json:"message"`
}

type ValidationReport struct {
	Valid                  bool         `json:"valid"`
	CompilerVersion        string       `json:"compiler_version"`
	PlatformContractDigest string       `json:"platform_contract_digest"`
	Diagnostics            []Diagnostic `json:"diagnostics"`
}

func NewValidationReport(compilerVersion, platformDigest string, diagnostics []Diagnostic) ValidationReport {
	ordered := append([]Diagnostic(nil), diagnostics...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		if left.Path != right.Path {
			return left.Path < right.Path
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if pointerString(left.Category) != pointerString(right.Category) {
			return pointerString(left.Category) < pointerString(right.Category)
		}
		if pointerString(left.Name) != pointerString(right.Name) {
			return pointerString(left.Name) < pointerString(right.Name)
		}
		return pointerString(left.NodeID) < pointerString(right.NodeID)
	})
	valid := true
	for _, diagnostic := range ordered {
		if diagnostic.Severity == SeverityError {
			valid = false
			break
		}
	}
	if ordered == nil {
		ordered = []Diagnostic{}
	}
	return ValidationReport{
		Valid: valid, CompilerVersion: compilerVersion,
		PlatformContractDigest: platformDigest, Diagnostics: ordered,
	}
}

func (r ValidationReport) WithDiagnostics(diagnostics ...Diagnostic) ValidationReport {
	return NewValidationReport(r.CompilerVersion, r.PlatformContractDigest,
		append(append([]Diagnostic(nil), r.Diagnostics...), diagnostics...))
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func diagnostic(
	code string,
	severity Severity,
	source DiagnosticSource,
	path, message string,
) Diagnostic {
	return Diagnostic{
		Code: code, Severity: severity, Source: source, Path: path, Message: message,
	}
}

func resourceDiagnostic(
	code string,
	severity Severity,
	source DiagnosticSource,
	path, category, name, message string,
) Diagnostic {
	categoryCopy, nameCopy := category, name
	return Diagnostic{
		Code: code, Severity: severity, Source: source, Path: path,
		Category: &categoryCopy, Name: &nameCopy, Message: message,
	}
}

func nodeResourceDiagnostic(
	code string,
	severity Severity,
	source DiagnosticSource,
	path, category, name, nodeID, message string,
) Diagnostic {
	diagnostic := resourceDiagnostic(code, severity, source, path, category, name, message)
	nodeCopy := nodeID
	diagnostic.NodeID = &nodeCopy
	return diagnostic
}
