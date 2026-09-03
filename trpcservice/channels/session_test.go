package channels

import "testing"

func TestRuntimeIdentityIsStableAndBindingScoped(t *testing.T) {
	userA, sessionA := RuntimeIdentity("binding-a", "user", "chat", "", "direct")
	userAgain, sessionAgain := RuntimeIdentity("binding-a", "user", "chat", "", "direct")
	if userA != userAgain || sessionA != sessionAgain {
		t.Fatal("identity is not stable")
	}
	userB, sessionB := RuntimeIdentity("binding-b", "user", "chat", "", "direct")
	if userA == userB || sessionA == sessionB {
		t.Fatal("identity leaked across bindings")
	}
}
