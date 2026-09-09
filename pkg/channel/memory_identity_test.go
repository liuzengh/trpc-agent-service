package channel

import (
	"strings"
	"testing"
)

func TestMemoryActorIDScopesEveryProviderIdentityComponent(t *testing.T) {
	base := []string{"tenant-a", "wework", "corp-a", "12345"}
	want, err := MemoryActorID(base[0], base[1], base[2], base[3])
	if err != nil || !strings.HasPrefix(want, "actor_") || len(want) != 70 {
		t.Fatalf("actor=%q err=%v", want, err)
	}
	for i := range base {
		changed := append([]string(nil), base...)
		changed[i] += "-other"
		got, err := MemoryActorID(changed[0], changed[1], changed[2], changed[3])
		if err != nil || got == want {
			t.Fatalf("component %d was not isolated: actor=%q err=%v", i, got, err)
		}
		changed[i] = ""
		if _, err := MemoryActorID(changed[0], changed[1], changed[2], changed[3]); err == nil {
			t.Fatalf("missing component %d accepted", i)
		}
	}
	again, _ := MemoryActorID(base[0], base[1], base[2], base[3])
	if again != want {
		t.Fatal("identity is not deterministic")
	}
}
