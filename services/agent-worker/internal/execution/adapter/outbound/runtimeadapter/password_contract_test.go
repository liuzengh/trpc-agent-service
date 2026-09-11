package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
)

// Control accepts a management DSN but resolves only its password. This test
// deliberately calls only preexisting Factory APIs so it also reproduces the
// regression against the pre-fix Factory without a compilation failure.
func TestFactoryControlPasswordOnlyContract(t *testing.T) {
	for name, password := range map[string]string{
		"special characters":   "fixture@:/%?# 密码",
		"URI is only password": "postgres://intruder:secret@other.invalid:9999/other?sslmode=disable",
	} {
		t.Run(name, func(t *testing.T) {
			g, p := runtimeFixture()
			p.SessionTarget.Host = "2001:db8::15"
			p.SessionTarget.Port = 6432
			p.SessionTarget.Database = "formal_session"
			p.SessionTarget.SSLMode = "verify-full"
			batch := responseFixture(g, p)
			for i := range batch.Credentials {
				if batch.Credentials[i].Purpose == "dsn" {
					batch.Credentials[i].Value = password
				}
			}
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(batch)
			})
			checks, opened := 0, 0
			f.openStore = func(_ context.Context, dsn string, target sessionstore.Target, capacity int) (candidateStore, error) {
				opened++
				if checks != 2 {
					t.Error("store constructed before the post-resolve grant check")
				}
				u, err := url.Parse(dsn)
				if err != nil || u.Scheme != "postgres" || u.User == nil {
					t.Error("password-only Control value was not assembled into a target-pinned PostgreSQL URL")
					return nil, errors.New("fixture invalid connection configuration")
				}
				actualPassword, ok := u.User.Password()
				if !ok || actualPassword != password || u.Hostname() != p.SessionTarget.Host || u.Port() != strconv.Itoa(int(p.SessionTarget.Port)) || u.User.Username() != p.SessionTarget.Username || u.Path != "/"+p.SessionTarget.Database || u.Query().Get("sslmode") != p.SessionTarget.SSLMode || len(u.Query()) != 1 || u.Fragment != "" {
					t.Error("resolved password changed destination or failed exact encoding round-trip")
					return nil, errors.New("fixture target mismatch")
				}
				if target.Host != p.SessionTarget.Host || target.Port != p.SessionTarget.Port || target.Database != p.SessionTarget.Database || target.Username != p.SessionTarget.Username || target.SSLMode != p.SessionTarget.SSLMode || capacity != 1<<20 {
					t.Error("constructor lost published destination or configured capacity")
				}
				return &fakeStore{}, nil
			}
			rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { checks++; return nil })
			if err != nil {
				t.Fatalf("password-only batch Prepare: %v", err)
			}
			rt.Close()
			if opened != 1 {
				t.Fatalf("constructors = %d, want 1", opened)
			}
		})
	}
}
