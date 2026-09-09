package summary

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestNewSummaryWorkSurvivesFinalAttemptFailureOrExpiry(t *testing.T) {
	for _, target := range []int64{0, 9} {
		for _, failure := range []string{"fail", "expire"} {
			t.Run(failure+"/target_"+strconv.FormatInt(target, 10), func(t *testing.T) {
				ctx := context.Background()
				now := time.Now().UTC()
				store := NewMemoryStore(func() time.Time { return now })
				request := summaryRequest(summaryKey(), 4)
				request.MaxAttempts = 1
				if _, err := store.Enqueue(ctx, request); err != nil {
					t.Fatal(err)
				}
				claimed, err := store.Claim(ctx, "worker-a", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				request.TargetEventSequence = target
				if _, err := store.Enqueue(ctx, request); err != nil {
					t.Fatal(err)
				}
				if failure == "fail" {
					if _, err := store.Fail(ctx, claimed, errors.New("generation failed"), now); err != nil {
						t.Fatal(err)
					}
				} else {
					now = now.Add(2 * time.Second)
				}
				next, err := store.Claim(ctx, "worker-b", time.Second)
				if err != nil || next.LeaseVersion <= claimed.LeaseVersion || next.Attempts != 1 {
					t.Fatalf("new target %d was stranded after %s: next=%#v err=%v", target, failure, next, err)
				}
				if _, err := store.Fail(ctx, next, errors.New("unchanged work failed"), now); err != nil {
					t.Fatal(err)
				}
				if _, err := store.Claim(ctx, "worker-c", time.Second); !errors.Is(err, ErrNoWork) {
					t.Fatalf("unchanged work exceeded its retry cap: %v", err)
				}
			})
		}
	}
}
