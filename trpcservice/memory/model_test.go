package memory

import "testing"

func validMemory() Memory {
	return Memory{TenantID: "tenant-a", ID: "memory-a", Scope: ScopeSession, ScopeID: "session-a", SessionID: "session-a", Kind: "fact", Content: "hello", Version: 1}
}

func TestMemoryValidation(t *testing.T) {
	value := validMemory()
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	value.Scope = ScopeUser
	value.SessionID = ""
	value.ScopeID = "user-a"
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	value.Scope = ScopeSession
	value.ScopeID = "other"
	if err := value.Validate(); err == nil {
		t.Fatal("expected session scope mismatch")
	}
}

func TestDeletedMemoryMayHaveEmptyContent(t *testing.T) {
	value := validMemory()
	value.Deleted = true
	value.Content = ""
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
}
