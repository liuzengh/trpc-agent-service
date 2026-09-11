package storage

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestArtifactObjectName(t *testing.T) {
	si := artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s1"}
	cases := []struct {
		name string
		want string
	}{
		{"report.pdf", "acme/u1/s1/report.pdf/1"},
		{"dir/x.txt", "acme/u1/s1/dir/x.txt/1"},
	}
	for _, c := range cases {
		if got := artifactObjectName(si, c.name, 1); got != c.want {
			t.Errorf("artifactObjectName(%q,1) = %q, want %q", c.name, got, c.want)
		}
	}
	// user-namespaced artifacts live under /user/ and skip the session segment
	if got, want := artifactObjectName(si, "user:avatar.png", 2), "acme/u1/user/user:avatar.png/2"; got != want {
		t.Errorf("user namespace name = %q, want %q", got, want)
	}
}

func TestArtifactTenantIsolation(t *testing.T) {
	a := artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s1"}
	b := artifact.SessionInfo{AppName: "other", UserID: "u1", SessionID: "s1"}
	if artifactObjectName(a, "f.txt", 0) == artifactObjectName(b, "f.txt", 0) {
		t.Error("artifact keys of different tenants must not collide")
	}
	if artifactSessionPrefix(a) == artifactSessionPrefix(b) {
		t.Error("session prefixes of different tenants must not collide")
	}
}

func TestParseArtifactRevision(t *testing.T) {
	cases := []struct {
		key string
		rev int
		ok  bool
	}{
		{"acme/u1/s1/f.txt/0", 0, true},
		{"acme/u1/s1/f.txt/12", 12, true},
		{"acme/u1/user/user:avatar/3", 3, true},
		{"acme/u1/s1/f.txt", 0, false},    // no revision leaf
		{"acme/u1/s1/f.txt/", 0, false},   // empty leaf
		{"acme/u1/s1/f.txt/v1", 0, false}, // non-numeric revision
	}
	for _, c := range cases {
		rev, ok := parseArtifactRevision(c.key)
		if ok != c.ok || (ok && rev != c.rev) {
			t.Errorf("parseArtifactRevision(%q) = (%d,%v), want (%d,%v)", c.key, rev, ok, c.rev, c.ok)
		}
	}
}
