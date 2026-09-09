package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestKMSResolver(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer kms-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ref := strings.TrimPrefix(r.URL.Path, "/v1/secrets/")
		switch ref {
		case "db-password":
			_, _ = w.Write([]byte("p4ss\n"))
		case "team/env/db-password":
			_, _ = w.Write([]byte("p4ss\n"))
		case "empty":
			_, _ = w.Write([]byte(""))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	r, err := NewKMSResolver(srv.URL, "kms-token")
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.Resolve(context.Background(), "db-password")
	if err != nil || v != "p4ss" {
		t.Fatalf("resolve: %v %q", err, v)
	}
	if _, err := r.Resolve(context.Background(), "missing"); err == nil {
		t.Fatal("missing secret must error")
	}
	if _, err := r.Resolve(context.Background(), "empty"); err == nil {
		t.Fatal("empty secret must error")
	}

	// Wrong token: the KMS rejects, the resolver surfaces it.
	bad, err := NewKMSResolver(srv.URL, "wrong")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Resolve(context.Background(), "db-password"); err == nil {
		t.Fatal("bad token must error")
	}

	// Path traversal and injection shapes are refused, not escaped: the ref
	// is interpolated into the request path verbatim.
	for _, bad := range []string{
		"", "../../v1/sys/config", "db-password?x=1", "db-password#frag",
		"/abs", "a/b/../c", "%2e%2e/db-password", "sp ace",
	} {
		if _, err := r.Resolve(context.Background(), bad); err == nil {
			t.Errorf("ref %q must be refused", bad)
		}
	}
	if _, err := r.Resolve(context.Background(), "team/env/db-password"); err != nil {
		t.Errorf("multi-segment ref must resolve: %v", err)
	}

	if _, err := NewKMSResolver("", "t"); err == nil {
		t.Fatal("endpoint required")
	}
}
