package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	"github.com/gowebpki/jcs"
)

func (i Intent) Validate() error {
	if err := validateAttachments(i.Attachments); err != nil {
		return err
	}
	for _, id := range []string{i.ID, i.AdmissionID, i.RunID, i.AttemptID, i.CompletionID} {
		if !identifier.MatchString(id) {
			return ErrInvalid
		}
	}
	if i.ExecutionGeneration < 1 || i.ExecutionGeneration > MaxExactInteger || i.Sequence < 1 || i.Sequence > MaxExactInteger || !textValid(i.Text) || !validTime(i.Deadline) {
		return ErrInvalid
	}
	return nil
}
func validTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }

// IntentDigest is the RFC 8785 SHA-256 of the stable Final v1 business document.
// The domain owns this representation independently of the generated wire DTO;
// a cross-boundary contract test requires byte-identical digests. Trace carriers
// and local target/owner capabilities are not business intent fields.
func IntentDigest(i Intent) (string, error) {
	if err := i.Validate(); err != nil {
		return "", err
	}
	content := struct {
		Type        string       `json:"type"`
		Text        string       `json:"text"`
		Attachments []Attachment `json:"attachments,omitempty"`
	}{"text", i.Text, i.Attachments}
	execution := struct {
		AttemptID    string `json:"attempt_id"`
		Generation   int64  `json:"generation"`
		CompletionID string `json:"completion_id"`
	}{i.AttemptID, i.ExecutionGeneration, i.CompletionID}
	document := struct {
		SchemaVersion int    `json:"schema_version"`
		IntentID      string `json:"intent_id"`
		AdmissionID   string `json:"admission_id"`
		RunID         string `json:"run_id"`
		Execution     any    `json:"execution"`
		Sequence      int64  `json:"sequence"`
		Kind          string `json:"kind"`
		Content       any    `json:"content"`
		Deadline      string `json:"deadline"`
	}{1, i.ID, i.AdmissionID, i.RunID, execution, i.Sequence, "final", content, i.Deadline.UTC().Format(time.RFC3339Nano)}
	return canonicalDigest(document)
}
func canonicalDigest(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", ErrInvalid
	}
	raw, err = jcs.Transform(raw)
	if err != nil {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (p Prepared) Validate() error {
	digest, err := IntentDigest(p.Intent)
	if err != nil || p.Digest != digest {
		return ErrInvalid
	}
	parts, err := Plan(p.Target, p.Intent)
	if err != nil {
		return err
	}
	if !slices.Equal(parts, p.Parts) {
		return ErrInvalid
	}
	return nil
}

// PartID is independent of claim/owner/retry identity. Invalid identifiers or
// indexes never produce a usable persistent identity.
func PartID(intentID string, index int) string {
	if !identifier.MatchString(intentID) || index < 0 || index >= MaxTextBytes {
		return ""
	}
	sum := sha256.Sum256([]byte("delivery-part-v1\x00" + intentID + "\x00" + strconv.Itoa(index)))
	return "part_" + hex.EncodeToString(sum[:])
}

func (r Result) Validate() error {
	switch r.ErrorClass {
	case ErrorNone, ErrorTemporary, ErrorPermanent, ErrorDeadline, ErrorStaleOrigin, ErrorRateLimited:
	default:
		return ErrInvalid
	}
	if r.ProviderMessageID != "" && !opaque(r.ProviderMessageID, 256) {
		return ErrInvalid
	}
	switch r.Certainty {
	case CertaintyAccepted:
		if r.ErrorClass != ErrorNone {
			return ErrInvalid
		}
	case CertaintyRejected:
		if r.ErrorClass == ErrorNone || r.ProviderMessageID != "" {
			return ErrInvalid
		}
	case CertaintyNotSent:
		if r.ErrorClass == ErrorNone || r.ProviderMessageID != "" {
			return ErrInvalid
		}
	case CertaintyUnknown:
		if r.ProviderMessageID != "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// RetryDelay permits at most three total calls, and only when the adapter knows
// the earlier request did not reach the Provider. A timeout/unknown outcome is
// never converted into proof of non-transmission.
func RetryDelay(result Result, attemptNumber int64, now, deadline time.Time) (time.Duration, bool) {
	if result.Validate() != nil || result.Certainty != CertaintyNotSent || (result.ErrorClass != ErrorTemporary && result.ErrorClass != ErrorRateLimited) || attemptNumber < 1 || attemptNumber >= 3 || !validTime(now) || !validTime(deadline) {
		return 0, false
	}
	delay := time.Duration(attemptNumber) * time.Second
	if !now.Add(delay).Before(deadline) {
		return 0, false
	}
	return delay, true
}

// RequestDigest binds the exact immutable intent, original destination, planned
// part, and local owner authority. It excludes scheduling deadlines and claim
// tokens; those are independently verified by the A2 transaction.
func RequestDigest(c Claim) (string, error) {
	digest, err := IntentDigest(c.Intent)
	if err != nil {
		return "", err
	}
	parts, err := Plan(c.Target, c.Intent)
	if err != nil {
		return "", err
	}
	if c.Part.IntentID != c.Intent.ID || c.Part.Index < 0 || c.Part.Index >= len(parts) || c.Part.ID != PartID(c.Intent.ID, c.Part.Index) || c.Part.Text != parts[c.Part.Index] {
		return "", ErrInvalid
	}
	if err := validateOwner(c.Target.Provider, c.InstanceID, c.Owner); err != nil {
		return "", err
	}
	target := c.Target
	target.ReceivedAt = target.ReceivedAt.UTC()
	return requestDigest(struct {
		Version      int         `json:"version"`
		IntentDigest string      `json:"intent_digest"`
		Target       Target      `json:"target"`
		PartID       string      `json:"part_id"`
		PartIndex    int         `json:"part_index"`
		Text         string      `json:"text"`
		InstanceID   string      `json:"instance_id"`
		Owner        *OwnerFence `json:"owner"`
	}{1, digest, target, c.Part.ID, c.Part.Index, c.Part.Text, c.InstanceID, c.Owner})
}
func validateOwner(provider, instanceID string, owner *OwnerFence) error {
	if !identifier.MatchString(instanceID) {
		return ErrInvalid
	}
	switch provider {
	case "telegram":
		if owner != nil {
			return ErrInvalid
		}
	case "wecom":
		if owner == nil || owner.InstanceID != instanceID || !identifier.MatchString(owner.InstanceID) || owner.Epoch < 1 || owner.Revision < 1 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func (r ClaimRequest) Validate() error {
	if !identifier.MatchString(r.AccountID) || r.Limit < 1 || r.Limit > 1000 || r.Lease < 100*time.Millisecond || r.Lease > 10*time.Minute {
		return ErrInvalid
	}
	return validateOwner(r.Provider, r.InstanceID, r.Owner)
}
func (r CallingRequest) Validate() error {
	if !opaque(r.RequestID, 256) || !digestPattern.MatchString(r.RequestDigest) || r.Timeout < time.Millisecond || r.Timeout > 10*time.Minute || !identifier.MatchString(r.Claim.Token) || !validTime(r.Claim.ExpiresAt) {
		return ErrInvalid
	}
	digest, err := RequestDigest(r.Claim)
	if err != nil || digest != r.RequestDigest {
		return ErrInvalid
	}
	return nil
}
func (o Observation) Validate() error {
	if !identifier.MatchString(o.ID) || !identifier.MatchString(o.AttemptID) || !opaque(o.EvidenceToken, 256) || !opaque(o.ProviderRequestID, 256) || !digestPattern.MatchString(o.RequestDigest) {
		return ErrInvalid
	}
	return o.Result.Validate()
}

// requestDigest hashes a versioned fixed Go struct representation. Unlike the
// cross-workload Intent JSON, this local request contains full-width int64/uint64
// fences. encoding/json preserves them exactly; JCS float normalization would
// collapse adjacent values above 2^53. The representation contains no maps.
func requestDigest(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
