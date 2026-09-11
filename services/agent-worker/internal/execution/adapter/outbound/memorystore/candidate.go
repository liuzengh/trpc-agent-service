// Package memorystore stores accepted SDK Memory snapshots, not tool CRUD.
package memorystore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"unicode/utf8"
)

var (
	ErrUnavailable = errors.New("memory storage unavailable")
	ErrIdentity    = errors.New("memory identity mismatch")
	ErrConflict    = errors.New("memory accepted revision conflict")
	ErrCorrupt     = errors.New("memory content integrity failure")
	ErrCapacity    = errors.New("memory snapshot exceeds configured capacity")
	ErrPreparation = errors.New("memory store requires explicit schema preparation")
)

type Scope struct {
	TenantID string `json:"tenant_id"`
	ID       string `json:"scope_id"`
}

func (s Scope) Key() memory.UserKey {
	return memory.UserKey{AppName: s.TenantID, UserID: s.ID}
}
func validID(s string) bool {
	return strings.TrimSpace(s) != "" && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func (s Scope) valid() bool { return validID(s.TenantID) && validID(s.ID) }

type Snapshot struct {
	Revision uint64
	Entries  []*memory.Entry
}

// SnapshotDigest gives migration verification the same canonical identity as
// normal accepted writes without exposing backend record formats.
func SnapshotDigest(scope Scope, snapshot Snapshot) (string, error) {
	if !scope.valid() || snapshot.Revision > math.MaxInt64 || (snapshot.Revision == 0 && len(snapshot.Entries) != 0) {
		return "", ErrIdentity
	}
	base := uint64(0)
	if snapshot.Revision > 0 {
		base = snapshot.Revision - 1
	}
	return (Candidate{Scope: scope, BaseRevision: base, Entries: snapshot.Entries}).Digest()
}

type Candidate struct {
	Scope        Scope           `json:"scope"`
	BaseRevision uint64          `json:"base_revision"`
	Entries      []*memory.Entry `json:"entries"`
}

// Accepted is supplied only by the completion owner after its durable accept.
// This store enforces identity and CAS, not remote completion authorization.
type Accepted struct{ CompletionID, RunID, AttemptID, CandidateDigest string }

func digest(b []byte) string { h := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(h[:]) }
func (c Candidate) encode() ([]byte, error) {
	if !c.Scope.valid() || c.BaseRevision >= math.MaxInt64 {
		return nil, ErrIdentity
	}
	entries := append([]*memory.Entry{}, c.Entries...)
	seen := map[string]bool{}
	for _, e := range entries {
		if e == nil || e.Memory == nil || !validID(e.ID) || seen[e.ID] || e.AppName != c.Scope.Key().AppName || e.UserID != c.Scope.Key().UserID {
			return nil, ErrIdentity
		}
		seen[e.ID] = true
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	c.Entries = entries
	b, err := json.Marshal(c)
	if err != nil {
		return nil, ErrCorrupt
	}
	return b, nil
}
func (c Candidate) Digest() (string, error) {
	b, err := c.encode()
	if err != nil {
		return "", err
	}
	return digest(b), nil
}
func decode(b []byte, scope Scope, expected string, capacity int) (Candidate, error) {
	if len(b) > capacity {
		return Candidate{}, ErrCapacity
	}
	var c Candidate
	if json.Unmarshal(b, &c) != nil || c.Scope != scope || digest(b) != expected {
		return Candidate{}, ErrCorrupt
	}
	encoded, err := c.encode()
	if err != nil || string(encoded) != string(b) {
		return Candidate{}, ErrCorrupt
	}
	return c, nil
}
