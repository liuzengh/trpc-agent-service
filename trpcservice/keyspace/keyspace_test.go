package keyspace

import "testing"

func TestSharedNamespaces(t *testing.T) {
	const prefix = " demo:: "
	if got, want := CoordinationPrefix(prefix), "demo:reliable-v1"; got != want {
		t.Fatalf("CoordinationPrefix()=%q, want %q", got, want)
	}
	if got, want := FencedPrefix(prefix), "demo:reliable-v1:fenced-v1"; got != want {
		t.Fatalf("FencedPrefix()=%q, want %q", got, want)
	}
	if got, want := FencedMeta(prefix, "coord"), "demo:reliable-v1:fenced-v1:session:coord:meta"; got != want {
		t.Fatalf("FencedMeta()=%q, want %q", got, want)
	}
}

func TestDigestCoordUsesLengthPrefixes(t *testing.T) {
	if DigestCoord("ab", "c") == DigestCoord("a", "bc") {
		t.Fatal("ambiguous concatenations produced the same coordinate")
	}
}
