package datav1

import (
	"bytes"
	"testing"
)

func TestRoleBindingKeepsSessionAndSeparatesMemory(t *testing.T) {
	for _, source := range fixtures()[:2] {
		t.Run(string(source.Kind), func(t *testing.T) {
			session, err := source.ForRole("session")
			if err != nil {
				t.Fatal(err)
			}
			old, _ := source.Canonical()
			got, _ := session.Canonical()
			if !bytes.Equal(old, got) {
				t.Fatal("Session canonical bytes changed")
			}
			memory, err := source.ForRole("memory")
			if err != nil || memory.ValidateForRole("memory") != nil {
				t.Fatal(memory, err)
			}
			if memory.Isolation != MemoryIsolation || session.Isolation != SessionIsolation || memory.Matches(session) {
				t.Fatal("role identity missing")
			}
			if memory.ValidateForRole("session") == nil || session.ValidateForRole("memory") == nil {
				t.Fatal("cross-role descriptor accepted")
			}
			if memory.BackendID != session.BackendID || memory.BackendRevision != session.BackendRevision {
				t.Fatal("physical target identity changed")
			}
			b, _ := memory.Canonical()
			decoded, err := Decode(b)
			if err != nil || !decoded.Matches(memory) {
				t.Fatal("Memory wire rejected", err)
			}
			if source.PostgreSQL != nil {
				memory.PostgreSQL.Host = "changed"
				if source.PostgreSQL.Host == "changed" {
					t.Fatal("alias")
				}
			} else {
				memory.Redis.Host = "changed"
				if source.Redis.Host == "changed" {
					t.Fatal("alias")
				}
			}
		})
	}
}
func TestRoleRejectsUnsupportedKindAndNames(t *testing.T) {
	for _, s := range fixtures() {
		if _, err := s.ForRole("unknown"); err == nil {
			t.Fatal("unknown role")
		}
		if s.Kind == Qdrant || s.Kind == S3 {
			if _, err := s.ForRole("memory"); err == nil {
				t.Fatal("wrong Memory kind")
			}
		}
	}
}
