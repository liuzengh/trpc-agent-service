package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func resourceSubject(user, id string) string {
	b, _ := json.Marshal([]string{user, id})
	return string(b)
}
func resourceHeld(ctx context.Context, app, kind string, subject ...string) bool {
	t, a, e := runtimecontext.ParseStorageScope(app)
	if e != nil {
		return false
	}
	if len(subject) == 0 {
		_, _, ok := controlplane.ResourceFromContext(ctx, t, a, kind)
		return ok
	}
	return controlplane.ResourceAccessHeld(ctx, t, a, kind, subject[0])
}

// Ordinary backend operations use a subject lock, while application-wide
// state operations, backfill and verification retain resourceValue's exclusive
// gate. Migration transitions therefore still exclude every in-flight write.
func resourceAccessValue[T any](ctx context.Context, repo controlplane.Repository, app, kind, subject string, write bool, fn func(context.Context) (T, error)) (out T, err error) {
	t, a, err := runtimecontext.ValidateStorageScope(ctx, app)
	if err != nil {
		return out, err
	}
	if subject == "" {
		return resourceValue(ctx, repo, app, kind, subject, write, fn)
	}
	store, ok := repo.(controlplane.ResourceAccessRepository)
	if !ok {
		return out, errors.New("subject resource coordination unavailable")
	}
	token, fenced := coordination.FencingTokenFromContext(ctx)
	err = store.WithResourceAccess(ctx, t, a, kind, controlplane.ResourceAccess{
		Subject: subject, Write: write, FencingToken: token, HasFencingToken: fenced && kind == "session",
	}, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		out, err = fn(ctx)
		return err
	})
	return out, err
}

func resourceAccessDo(ctx context.Context, repo controlplane.Repository, app, kind, subject string, write bool, fn func(context.Context) error) error {
	_, err := resourceAccessValue(ctx, repo, app, kind, subject, write, func(ctx context.Context) (struct{}, error) { return struct{}{}, fn(ctx) })
	return err
}
func resourceValue[T any](ctx context.Context, repo controlplane.Repository, app, kind, subject string, write bool, fn func(context.Context) (T, error)) (out T, err error) {
	t, a, err := runtimecontext.ValidateStorageScope(ctx, app)
	if err != nil {
		return out, err
	}
	store, ok := repo.(controlplane.ResourceSyncRepository)
	if !ok {
		return out, errors.New("resource coordination unavailable")
	}
	err = store.WithResourceSync(ctx, t, a, kind, func(ctx context.Context, s *controlplane.ResourceSync, save func() error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		changed := false
		if subject != "" && !s.Subjects[subject] {
			s.Subjects[subject] = true
			s.Epoch++
			changed = true
		}
		if token, ok := coordination.FencingTokenFromContext(ctx); kind == "session" && ok && subject != "" {
			if token < s.Fences[subject] {
				return coordination.ErrLeaseLost
			}
			if token > s.Fences[subject] {
				s.Fences[subject] = token
				changed = true
			}
		}
		if write {
			s.Epoch++
			changed = true
		}
		if changed {
			if err := save(); err != nil {
				return err
			}
		}
		var err error
		out, err = fn(ctx)
		return err
	})
	return out, err
}
func resourceState(ctx context.Context, app, kind string) (*controlplane.ResourceSync, func() error, error) {
	t, a, err := runtimecontext.ValidateStorageScope(ctx, app)
	if err != nil {
		return nil, nil, err
	}
	s, save, ok := controlplane.ResourceFromContext(ctx, t, a, kind)
	if !ok {
		return nil, nil, errors.New("resource operation requires ownership")
	}
	return s, save, nil
}
func dataDigest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}
func rejectPrivateState(state map[string][]byte) error {
	for key := range state {
		if strings.HasPrefix(key, "_platform:") {
			return errors.New("reserved platform Session state key")
		}
	}
	return nil
}
