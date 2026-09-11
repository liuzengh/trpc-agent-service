package sessionstore

import (
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCredentialDSNPasswordNeverSelectsDestination(t *testing.T) {
	for _, host := range []string{"session.internal", "127.0.0.1", "2001:db8::1"} {
		for _, mode := range []string{"disable", "require", "verify-full"} {
			for _, password := range []string{"fixture-only", "@:/%?# 秘密", "postgres://other:secret@other.invalid:9999/other?sslmode=disable", "host=other.invalid user=other password=other"} {
				target := Target{Host: host, Port: 5433, Database: "agent ?#% state", Username: "session_runtime", SSLMode: mode}
				dsn, err := CredentialDSN(target, password)
				if err != nil {
					t.Fatal(err)
				}
				u, err := url.Parse(dsn)
				if err != nil {
					t.Fatal("assembled URL was invalid")
				}
				gotPassword, ok := u.User.Password()
				if !ok || gotPassword != password || u.Hostname() != target.Host || u.Port() != "5433" || u.User.Username() != target.Username || u.Path != "/"+target.Database || u.Query().Get("sslmode") != mode || len(u.Query()) != 1 || u.Fragment != "" {
					t.Fatal("password changed the immutable destination or failed round-trip")
				}
				cfg, err := pgxpool.ParseConfig(dsn)
				if err != nil {
					t.Fatal("assembled URL failed driver parsing")
				}
				if cfg.ConnConfig.Host != target.Host || cfg.ConnConfig.Port != target.Port || cfg.ConnConfig.User != target.Username || cfg.ConnConfig.Database != target.Database || cfg.ConnConfig.Password != password {
					t.Fatal("driver parsed a destination not pinned by target")
				}
			}
		}
	}
}

func TestCredentialDSNRejectsInvalidTargetAndEmptyPassword(t *testing.T) {
	good := Target{Host: "session.internal", Port: 5432, Database: "agent_platform", Username: "session_runtime", SSLMode: "require"}
	changes := map[string]func(*Target){
		"blank host":       func(t *Target) { t.Host = "" },
		"long host":        func(t *Target) { t.Host = strings.Repeat("h", 254) },
		"host override":    func(t *Target) { t.Host = "session.internal,other.invalid" },
		"host URL":         func(t *Target) { t.Host = "postgres://other.invalid" },
		"unix socket":      func(t *Target) { t.Host = "/tmp/pg" },
		"host params":      func(t *Target) { t.Host = "session.internal?host=other.invalid" },
		"bracket host":     func(t *Target) { t.Host = "[::1]" },
		"host port":        func(t *Target) { t.Host = "session.internal:9999" },
		"host newline":     func(t *Target) { t.Host += "\n" },
		"port":             func(t *Target) { t.Port = 0 },
		"blank database":   func(t *Target) { t.Database = "" },
		"long database":    func(t *Target) { t.Database = strings.Repeat("d", 129) },
		"database slash":   func(t *Target) { t.Database = "a/b" },
		"database control": func(t *Target) { t.Database = "a\x00b" },
		"role":             func(t *Target) { t.Username = "postgres" },
		"mode":             func(t *Target) { t.SSLMode = "prefer" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			target := good
			change(&target)
			if dsn, err := CredentialDSN(target, "fixture-secret"); !errors.Is(err, ErrIdentity) || dsn != "" {
				t.Fatal("invalid destination generated a DSN")
			}
		})
	}
	for _, password := range []string{"", " \t", "x\x00x", "x\nx", "x\rx", string([]byte{0xff})} {
		if dsn, err := CredentialDSN(good, password); !errors.Is(err, ErrIdentity) || dsn != "" {
			t.Fatal("invalid password generated a DSN")
		}
	}
}
