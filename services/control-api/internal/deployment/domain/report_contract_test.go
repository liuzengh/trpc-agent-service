package domain

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/gowebpki/jcs"
)

func reportContractExamples() map[string]ValidationReport {
	input := validCompileInput()
	delete(input.Agent.Spec.Requirements.Tools, "debug")
	_, valid := Compile(input)
	_, warning := Compile(validCompileInput())
	delete(input.Profile.Spec.Tools, "search")
	_, missing := Compile(input)
	unavailable := valid.WithDiagnostics(Diagnostic{
		Code: DiagnosticCredentialUnavailable, Severity: SeverityError, Source: DiagnosticSourceProfile,
		Path: "/credentials", Message: "required profile credentials are unavailable",
	})
	return map[string]ValidationReport{"valid.json": valid, "unused-warning.json": warning, "missing-resource.json": missing, "credential-unavailable.json": unavailable}
}

func TestValidationReportGoldenContract(t *testing.T) {
	for name, report := range reportContractExamples() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "..", "..", "api", "openapi", "control", "v1", "examples", "deployment-reports", name)
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := jcs.Transform(raw)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := jcs.Transform(want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("report contract changed: %s", actual)
			}
		})
	}
}
