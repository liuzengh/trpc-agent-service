package knowledge

import (
	"context"
	"reflect"
	"testing"
)

func TestDeterministicEmbedderHasNoCredentialOrNetworkDependency(t *testing.T) {
	embedder := DeterministicEmbedder{Dimensions: 4}
	first, err := embedder.GetEmbedding(context.Background(), "local fixture")
	if err != nil {
		t.Fatal(err)
	}
	second, err := embedder.GetEmbedding(context.Background(), "local fixture")
	if err != nil || len(first) != 4 || !reflect.DeepEqual(first, second) {
		t.Fatalf("first=%v second=%v err=%v", first, second, err)
	}
}
