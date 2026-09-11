// Package sessionstore persists immutable attempt candidates, never accepted heads.
// Execution alone decides which candidate becomes the next formal session.
package sessionstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

const ContentVersion = "worker-session-v1"

var (
	ErrIdentity    = errors.New("session candidate identity mismatch")
	ErrConflict    = errors.New("session candidate immutable key conflict")
	ErrNotFound    = errors.New("session candidate not found")
	ErrCorrupt     = errors.New("session candidate content integrity failure")
	ErrCapacity    = errors.New("session snapshot exceeds configured capacity")
	ErrPreparation = errors.New("session store requires explicit schema preparation")
)

// Identity is assigned by Execution, not by end-user input or the SDK.
type Identity struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
	AttemptID string `json:"attempt_id"`
}

type Head struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

// Candidate is one complete SDK session snapshot plus its accepted parent.
// Snapshot is JSON but stored as bytes: PostgreSQL must not normalize its digest.
type Candidate struct {
	Identity       Identity        `json:"identity"`
	Parent         Head            `json:"parent"`
	ContentVersion string          `json:"content_version"`
	Snapshot       json.RawMessage `json:"snapshot"`
}

// Store deliberately has neither a latest query nor an overwrite operation.
type Store interface {
	Load(context.Context, string, string, Head) (Candidate, error)
	Put(context.Context, Candidate) (Head, error)
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (h Head) Validate() error {
	if h.Ref == "" && h.Digest == "" {
		return nil
	}
	if len(h.Ref) != 68 || h.Ref[:4] != "sc1_" || !digestPattern.MatchString(h.Ref[4:]) || (len(h.Digest) != 71 || h.Digest[:7] != "sha256:" || !digestPattern.MatchString(h.Digest[7:])) {
		return ErrIdentity
	}
	return nil
}

func (c Candidate) Encode(capacityBytes int) ([]byte, Head, error) {
	if capacityBytes <= 0 {
		return nil, Head{}, fmt.Errorf("positive session capacity is required")
	}
	for _, s := range []string{c.Identity.TenantID, c.Identity.SessionID, c.Identity.RunID, c.Identity.AttemptID} {
		if s == "" || len(s) > 256 {
			return nil, Head{}, ErrIdentity
		}
	}
	if c.ContentVersion != ContentVersion {
		return nil, Head{}, ErrCorrupt
	}
	if err := c.Parent.Validate(); err != nil {
		return nil, Head{}, err
	}
	if !json.Valid(c.Snapshot) {
		return nil, Head{}, ErrCorrupt
	}
	if len(c.Snapshot) > capacityBytes {
		return nil, Head{}, ErrCapacity
	}
	// JSON marshaling compacts RawMessage identically on writes and replay.
	body, err := json.Marshal(c)
	if err != nil {
		return nil, Head{}, ErrCorrupt
	}
	// Account for JSON escaping before durability, so an encoded candidate is
	// always readable with the same configured capacity.
	var encoded Candidate
	if err := json.Unmarshal(body, &encoded); err != nil {
		return nil, Head{}, ErrCorrupt
	}
	if len(encoded.Snapshot) > capacityBytes {
		return nil, Head{}, ErrCapacity
	}
	identity, _ := json.Marshal(c.Identity)
	key := sha256.Sum256(identity)
	sum := sha256.Sum256(body)
	return body, Head{Ref: "sc1_" + hex.EncodeToString(key[:]), Digest: "sha256:" + hex.EncodeToString(sum[:])}, nil
}

func Decode(body []byte, tenantID, sessionID string, head Head, capacityBytes int) (Candidate, error) {
	var c Candidate
	if err := json.Unmarshal(body, &c); err != nil {
		return c, ErrCorrupt
	}
	encoded, actual, err := c.Encode(capacityBytes)
	if err != nil {
		if errors.Is(err, ErrCapacity) {
			return c, err
		}
		return c, ErrCorrupt
	}
	if actual != head || string(encoded) != string(body) {
		return c, ErrCorrupt
	}
	if c.Identity.TenantID != tenantID || c.Identity.SessionID != sessionID {
		return c, ErrIdentity
	}
	return c, nil
}
