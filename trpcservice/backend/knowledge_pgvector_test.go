package backend

import (
	"math"
	"strings"
	"testing"
)

func TestVectorLiteralIsDeterministicAndRejectsNoSQLSurface(t *testing.T) {
	got := vectorLiteral([]float64{1, -0.25, math.SmallestNonzeroFloat64})
	if !strings.HasPrefix(got, "[1,-0.25,") || !strings.HasSuffix(got, "]") {
		t.Fatalf("vector literal = %q", got)
	}
	for _, value := range []string{"knowledge_1536", "a", "tenant1_docs"} {
		if !safeIdentifier(value) {
			t.Fatalf("safe identifier %q rejected", value)
		}
	}
	for _, value := range []string{"", "Knowledge", "public.docs", "docs;drop table x", "1docs"} {
		if safeIdentifier(value) {
			t.Fatalf("unsafe identifier %q accepted", value)
		}
	}
	if finiteVector([]float64{1, math.Inf(1)}) || finiteVector([]float64{math.NaN()}) {
		t.Fatal("non-finite embedding was accepted")
	}
}

func TestNewPostgresKnowledgeRejectsInvalidConfigBeforeOpeningDatabase(t *testing.T) {
	_, err := NewPostgresKnowledge(nil, PostgresKnowledgeOptions{DSN: "secret", Table: "bad.table"})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("invalid configuration error = %v", err)
	}
}

func TestPreparePostgresKnowledgeRejectsUnsafeTarget(t *testing.T) {
	if err := PreparePostgresKnowledge(nil, nil, "public.docs", 1536); err == nil {
		t.Fatal("unsafe vector table was accepted")
	}
}
