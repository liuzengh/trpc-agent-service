package telegramreception

import (
	"testing"
	"time"
)

func TestIdleRecoveryUsesNonAcknowledgingOffsetZero(t *testing.T) {
	now := time.Now()
	l := Lease{NextOffset: 999999, LastUpdateAt: now.Add(-IdleRecoveryAfter)}
	if l.PollOffset(now) != 0 || l.NextOffset != 999999 {
		t.Fatal("must probe without mutating durable cursor")
	}
	l.LastUpdateAt = now.Add(-IdleRecoveryAfter + time.Second)
	if l.PollOffset(now) != 999999 {
		t.Fatal("premature reset")
	}
}
func TestOnlyKnownManagedRemoteCanBeChanged(t *testing.T) {
	for _, v := range []struct {
		actual, managed string
		want            bool
	}{{"", "", true}, {"https://ours", "https://ours", true}, {"https://foreign", "", false}, {"https://foreign", "https://ours", false}} {
		if ManagedRemote(v.actual, v.managed) != v.want {
			t.Fatal(v)
		}
	}
}
