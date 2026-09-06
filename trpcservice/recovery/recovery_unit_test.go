package recovery

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type stubRunner struct {
	toc string
	err error
}

func (r stubRunner) Run(_ context.Context, _ ClientTool, _ []string, _ []string, _ []Mount, stdout io.Writer) error {
	if r.err != nil {
		return r.err
	}
	if stdout != nil && r.toc != "" {
		_, _ = io.WriteString(stdout, r.toc)
	}
	return nil
}

func TestAuditArchiveTOCRejectsNonDataOnlyEntries(t *testing.T) {
	manifest := Manifest{Schema: "public", Tables: []TableStat{
		{Name: "tenant", Rows: 1}, {Name: "session", Rows: 2},
	}}
	toc := "; header\n" +
		"-- Data for Name: tenant; Type: TABLE DATA; Schema: public; Owner: owner\n" +
		"3525; 0 16393 TABLE DATA public tenant owner\n" +
		"-- Data for Name: session; Type: TABLE DATA; Schema: public; Owner: owner\n" +
		"3526; 0 16396 TABLE DATA public session owner\n"
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: toc}, "/x/archive", manifest); err != nil {
		t.Fatalf("valid data-only TOC rejected: %v", err)
	}
	withSchema := toc + "3527; 1259 16400 SCHEMA public owner\n"
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: withSchema}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("SCHEMA entry accepted: %v", err)
	}
	withFunction := toc + "3528; 1255 16402 FUNCTION public f() owner\n"
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: withFunction}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("FUNCTION entry accepted: %v", err)
	}
	withACL := toc + "3529; 0 0 ACL public tenant owner\n"
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: withACL}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("ACL entry accepted: %v", err)
	}
	withSequence := toc + "3530; 0 0 SEQUENCE SET public s owner\n"
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: withSequence}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("SEQUENCE entry accepted: %v", err)
	}
	withWrongSchema := strings.Replace(toc, "TABLE DATA public tenant", "TABLE DATA other tenant", 1)
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: withWrongSchema}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("wrong schema accepted: %v", err)
	}
	missing := strings.Replace(toc, "3526; 0 16396 TABLE DATA public session owner\n", "", 1)
	if _, err := auditArchiveTOC(context.Background(), stubRunner{toc: missing}, "/x/archive", manifest); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("missing table accepted: %v", err)
	}
}

func TestParseDSNRejectsNonURLAndUnknownParams(t *testing.T) {
	if _, err := parseDSN("host=127.0.0.1 user=x"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("keyword dsn accepted: %v", err)
	}
	if _, err := parseDSN("mysql://x:y@h/db"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("foreign scheme accepted: %v", err)
	}
	if _, err := parseDSN("postgres://u:p@h/db?application_name=x"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown query param accepted: %v", err)
	}
	params, err := parseDSN("postgres://u:p%40ss@h:5433/db?sslmode=disable")
	if err != nil {
		t.Fatalf("valid dsn rejected: %v", err)
	}
	if params.Host != "h" || params.Port != "5433" || params.User != "u" || params.Database != "db" || params.SSLMode != "disable" {
		t.Fatalf("parsed params wrong: %+v", params)
	}
}

func TestWritePGPassFileModeAndCleanup(t *testing.T) {
	env, cleanup, err := writePGPassFile(connParams{Host: "h", Port: "5432", Database: "d", User: "u", Password: "p:\\w"})
	if err != nil {
		t.Fatal(err)
	}
	if len(env) != 1 || !startsWith(env[0], "PGPASSFILE=") {
		t.Fatalf("pgpass env missing")
	}
	file := env[0][len("PGPASSFILE="):]
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("pgpass mode: %v", info.Mode().Perm())
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "h:5432:d:u:p\\:\\\\w\n" {
		t.Fatalf("pgpass escaping wrong: %q", string(raw))
	}
	cleanup()
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("pgpass cleanup failed")
	}
	if _, _, err := writePGPassFile(connParams{Host: "h", Port: "1", Database: "d", User: "u"}); err != nil {
		t.Fatalf("passwordless dsn rejected: %v", err)
	}
}

func TestMigrationDigestIsStableAndOrderSensitive(t *testing.T) {
	a := []MigrationEntry{{Version: 1, Name: "n", Checksum: "c1"}}
	b := []MigrationEntry{{Version: 1, Name: "n", Checksum: "c1"}}
	if MigrationDigest(a) != MigrationDigest(b) {
		t.Fatalf("digest unstable")
	}
	c := []MigrationEntry{{Version: 1, Name: "n", Checksum: "c2"}}
	if MigrationDigest(a) == MigrationDigest(c) {
		t.Fatalf("digest insensitive to checksum")
	}
}

func TestSourceIdentityFingerprintHidesCoordinates(t *testing.T) {
	f1 := sourceIdentityFingerprint("host", "5432", "db")
	f2 := sourceIdentityFingerprint("host", "5432", "db")
	f3 := sourceIdentityFingerprint("host", "5433", "db")
	if f1 != f2 || f1 == f3 {
		t.Fatalf("source identity fingerprint unstable")
	}
	if len(f1) != 64 || strings.Contains(f1, "host") {
		t.Fatalf("fingerprint leaks coordinates or is malformed: %q", f1)
	}
}

func TestReadPublishedBackupRejectsBrokenDirectories(t *testing.T) {
	dir := t.TempDir()
	if _, err := readPublishedBackup(dir); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("missing marker accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, BackupDirMarker), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPublishedBackup(dir); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("missing archive accepted: %v", err)
	}
}

func TestValidEventIDValueMirrorsDomainRule(t *testing.T) {
	for _, ok := range []string{"a", "evt-1", "a_b"} {
		if !validEventIDValue(ok) {
			t.Fatalf("valid id rejected: %s", ok)
		}
	}
	for _, bad := range []string{"", " a", "a|b", "a/b", "a\\b", string(rune(0))} {
		if validEventIDValue(bad) {
			t.Fatalf("invalid id accepted: %q", bad)
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
