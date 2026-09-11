package memorystore

import (
	"context"
	"errors"
	"testing"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

func TestMemoryCandidateCanonicalScope(t *testing.T) {
	scope := Scope{TenantID: "tenant", ID: "scope"}
	sdk := inmemory.NewMemoryService()
	defer sdk.Close()
	sdk.AddMemory(context.Background(), scope.Key(), "tea", nil)
	sdk.AddMemory(context.Background(), scope.Key(), "coffee", nil)
	entries, _ := sdk.ReadMemories(context.Background(), scope.Key(), 0)
	c := Candidate{Scope: scope, Entries: entries}
	first, err := c.Digest()
	if err != nil {
		t.Fatal(err)
	}
	c.Entries = []*memory.Entry{entries[1], entries[0]}
	second, _ := c.Digest()
	if first != second {
		t.Fatal("order changed digest")
	}
	c.Entries = []*memory.Entry{entries[0], entries[0]}
	if _, err = c.Digest(); !errors.Is(err, ErrIdentity) {
		t.Fatal("duplicate accepted", err)
	}
	c = Candidate{Scope: scope}
	nilHash, _ := c.Digest()
	c.Entries = []*memory.Entry{}
	emptyHash, _ := c.Digest()
	if nilHash != emptyHash {
		t.Fatal("empty clear differs")
	}
	target := Target{Host: "localhost", Port: 5432, Database: "memory", Username: "memory_runtime", SSLMode: "disable"}
	if _, err = CredentialDSN(target, "password"); err != nil {
		t.Fatal(err)
	}
	target.Username = "session_runtime"
	if _, err = CredentialDSN(target, "password"); !errors.Is(err, ErrIdentity) {
		t.Fatal("wrong runtime role", err)
	}
}

func TestStorageFailureDoesNotExposeDriverMessage(t *testing.T) {
	if got := storageFailure(errors.New("postgres://secret:password@private/database")); got != ErrUnavailable {
		t.Fatal(got)
	}
	if storageFailure(context.Canceled) != context.Canceled {
		t.Fatal("cancel classification lost")
	}
}
