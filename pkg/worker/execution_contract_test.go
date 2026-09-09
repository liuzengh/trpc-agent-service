package worker

import (
	"errors"
	"testing"
	"time"
)

func TestExecutionContractRequiresExactVersionAndCanonicalTimeout(t *testing.T) {
	configured := 90 * time.Second
	valid, err := NewExecutionContract(configured)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionContract(&valid, configured); err != nil {
		t.Fatalf("valid contract rejected: %v", err)
	}

	tests := []struct {
		name     string
		contract *ExecutionContract
		worker   time.Duration
	}{
		{name: "missing", worker: configured},
		{name: "unknown version", contract: &ExecutionContract{Version: 2, Timeout: "1m30s"}, worker: configured},
		{name: "non-canonical timeout", contract: &ExecutionContract{Version: 1, Timeout: "90s"}, worker: configured},
		{name: "malformed timeout", contract: &ExecutionContract{Version: 1, Timeout: "later"}, worker: configured},
		{name: "unsafe timeout", contract: &ExecutionContract{Version: 1, Timeout: "500ms"}, worker: configured},
		{name: "configuration drift", contract: &ExecutionContract{Version: 1, Timeout: "1m30s"}, worker: time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateExecutionContract(test.contract, test.worker); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
				t.Fatalf("error=%v, want ErrWorkerExecutionBudgetInvalid", err)
			}
		})
	}
}

func TestNewExecutionContractPreservesSubSecondPrecision(t *testing.T) {
	configured := 90*time.Second + 250*time.Millisecond
	contract, err := NewExecutionContract(configured)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Timeout != "1m30.25s" {
		t.Fatalf("timeout=%q, want canonical precision", contract.Timeout)
	}
	if err := ValidateExecutionContract(&contract, configured); err != nil {
		t.Fatal(err)
	}
}
