// Package summary owns immutable summary bodies and their safe compaction.
// Session boundaries remain in storage/session and are never deleted here.
package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Key struct{ TenantID, AgentAppID, SessionID, SummaryID string }

type Body struct {
	Key
	ContentRef, ContentDigest string
	Content                   []byte
	CreatedAt                 time.Time
}

type ClaimedBody struct {
	Body
	SupersededBy  string
	ClaimOwner    string
	ClaimUntil    time.Time
	DeleteAttempt int
	Version       int64
}

type Store interface {
	Put(context.Context, Body) (Body, error)
	Get(context.Context, Key) (Body, error)
	// Supersede declares that replacement has a strictly higher already-committed
	// session watermark. It never mutates the source body.
	Supersede(context.Context, Key, string, time.Time) error
	ClaimSuperseded(context.Context, time.Time, string, time.Duration, int) ([]ClaimedBody, error)
	FinishDelete(context.Context, ClaimedBody) error
	DeferDelete(context.Context, ClaimedBody, time.Time, string) error
}

func ValidateBody(value Body) (Body, error) {
	if value.TenantID == "" || value.AgentAppID == "" || value.SessionID == "" || value.SummaryID == "" ||
		value.ContentRef == "" || len(value.Content) == 0 || len(value.Content) > 1<<20 {
		return Body{}, runtime.ErrInvalidEnvelope
	}
	sum := sha256.Sum256(value.Content)
	if value.ContentDigest == "" {
		value.ContentDigest = hex.EncodeToString(sum[:])
	}
	if value.ContentDigest != hex.EncodeToString(sum[:]) {
		return Body{}, runtime.ErrIdempotencyCollision
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	value.Content = append([]byte(nil), value.Content...)
	return value, nil
}
