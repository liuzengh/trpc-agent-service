package tenant

import (
	"context"
	"errors"
	"testing"
)

func TestLoadKeyRingSupportsRollingMigration(t *testing.T) {
	lookup := func(key string) (string, bool) {
		values := map[string]string{
			"MASTER_KEY_RING":      `{"v2":"new-master-material-which-is-long-enough"}`,
			"MASTER_KEY":           "old-master-material-which-is-long-enough",
			"ACTIVE_MASTER_KEY_ID": "v2",
		}
		value, ok := values[key]
		return value, ok
	}
	active, ring, err := LoadKeyRing(lookup, 32)
	if err != nil {
		t.Fatal(err)
	}
	if active != "v2" || ring["v1"] == "" || ring["v2"] == "" {
		t.Fatalf("active=%q ring=%v", active, ring)
	}
}

func TestLoadKeyRingRejectsWeakOrMissingActiveKey(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "missing", values: map[string]string{}},
		{name: "weak", values: map[string]string{"MASTER_KEY": "too-short"}},
		{name: "unknown active", values: map[string]string{
			"MASTER_KEY_RING":      `{"v1":"01234567890123456789012345678901"}`,
			"ACTIVE_MASTER_KEY_ID": "v2",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := LoadKeyRing(func(key string) (string, bool) {
				value, ok := test.values[key]
				return value, ok
			}, 32)
			if err == nil {
				t.Fatal("expected key-ring configuration error")
			}
		})
	}
}

func TestLoadKeyRingWithResolverSupportsSecretReferences(t *testing.T) {
	resolver, err := NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"MASTER_KEY_RING_REF":  "env://TRPC_SECRET_RING",
		"ACTIVE_MASTER_KEY_ID": "v2",
	}
	secrets := map[string]string{
		"TRPC_SECRET_RING": `{"v1":"old-master-material-which-is-long-enough","v2":"new-master-material-which-is-long-enough"}`,
	}
	resolverLookup := &EnvSecretResolver{
		prefix: resolver.prefix,
		lookupEnv: func(name string) (string, bool) {
			value, ok := secrets[name]
			return value, ok
		},
	}
	active, ring, err := LoadKeyRingWithResolver(
		context.Background(),
		func(name string) (string, bool) { value, ok := values[name]; return value, ok },
		resolverLookup,
		32,
	)
	if err != nil {
		t.Fatal(err)
	}
	if active != "v2" || ring["v1"] == "" || ring["v2"] == "" {
		t.Fatalf("active=%q ring=%v", active, ring)
	}
}

func TestLoadKeyRingFromEnvResolvesScopedReference(t *testing.T) {
	t.Setenv("MASTER_KEY_RING", "")
	t.Setenv("MASTER_KEY", "")
	t.Setenv("MASTER_KEY_RING_REF", "env://TRPC_SECRET_RING")
	t.Setenv("TRPC_SECRET_RING", `{"v1":"old-master-material-which-is-long-enough"}`)
	t.Setenv("ACTIVE_MASTER_KEY_ID", "v1")
	active, ring, err := LoadKeyRingFromEnv(32)
	if err != nil {
		t.Fatal(err)
	}
	if active != "v1" || ring["v1"] == "" {
		t.Fatalf("active=%q ring=%v", active, ring)
	}
}

func TestLoadKeyRingWithResolverRejectsAmbiguousOrUnavailableReferences(t *testing.T) {
	resolver, err := NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		values map[string]string
		want   error
	}{
		{
			name: "ambiguous ring source",
			values: map[string]string{
				"MASTER_KEY_RING":     `{"v1":"old-master-material-which-is-long-enough"}`,
				"MASTER_KEY_RING_REF": "env://TRPC_SECRET_RING",
			},
			want: nil,
		},
		{
			name: "resolver missing",
			values: map[string]string{
				"MASTER_KEY_REF": "env://TRPC_SECRET_MISSING",
			},
			want: ErrSecretUnavailable,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := LoadKeyRingWithResolver(context.Background(), func(name string) (string, bool) {
				value, ok := test.values[name]
				return value, ok
			}, resolver, 32)
			if test.want == nil {
				if err == nil {
					t.Fatal("ambiguous source unexpectedly accepted")
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}
