package executionv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

const AttemptVerifyPath = "/internal/v1/execution/attempts:verify"
const MaxAttemptProofBytes = 64 * 1024

var ErrInvalidAttemptProof = errors.New("invalid active Attempt proof")

type CredentialUse struct {
	CredentialID   string `json:"credential_id"`
	Purpose        string `json:"purpose"`
	AudienceDigest string `json:"audience_digest"`
}
type AttemptRequest struct {
	WorkloadIdentity string `json:"workload_identity"`
	ExecutionToken   string `json:"execution_token"`
	ManifestID       string `json:"manifest_id"`
	ManifestDigest   string `json:"manifest_digest"`
}
type AttemptResponse struct {
	TenantID              string          `json:"tenant_id"`
	ProfileID             string          `json:"profile_id"`
	ProfileRevisionNumber int64           `json:"profile_revision_number"`
	RunID                 string          `json:"run_id"`
	AttemptID             string          `json:"attempt_id"`
	WorkerID              string          `json:"worker_id"`
	LeaseEpoch            int64           `json:"lease_epoch"`
	ExpiresAt             time.Time       `json:"expires_at"`
	ManifestID            string          `json:"manifest_id"`
	ManifestDigest        string          `json:"manifest_digest"`
	AllowedUses           []CredentialUse `json:"allowed_uses"`
}

func (r AttemptRequest) Validate() error {
	if r.WorkloadIdentity == "" || len(r.WorkloadIdentity) > 256 || !finalID.MatchString(r.ManifestID) || !finalDigest.MatchString(r.ManifestDigest) || len(r.ExecutionToken) < 1 || len(r.ExecutionToken) > 8192 {
		return ErrInvalidAttemptProof
	}
	return nil
}
func (r AttemptResponse) Validate() error {
	for _, id := range []string{r.TenantID, r.ProfileID, r.RunID, r.AttemptID, r.ManifestID} {
		if !finalID.MatchString(id) {
			return ErrInvalidAttemptProof
		}
	}
	if r.WorkerID == "" || len(r.WorkerID) > 256 || r.LeaseEpoch < 1 || r.LeaseEpoch > 9007199254740991 || r.ProfileRevisionNumber < 1 || r.ProfileRevisionNumber > 9007199254740991 || r.ExpiresAt.IsZero() || !finalDigest.MatchString(r.ManifestDigest) || r.AllowedUses == nil || len(r.AllowedUses) > 256 {
		return ErrInvalidAttemptProof
	}
	seen := map[string]bool{}
	for _, u := range r.AllowedUses {
		if !finalID.MatchString(u.CredentialID) || len(u.Purpose) < 1 || len(u.Purpose) > 64 || !finalDigest.MatchString(u.AudienceDigest) || seen[u.CredentialID] {
			return ErrInvalidAttemptProof
		}
		seen[u.CredentialID] = true
	}
	return nil
}
func EncodeAttemptRequest(r AttemptRequest) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}
func EncodeAttemptResponse(r AttemptResponse) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}
func DecodeAttemptRequest(raw []byte) (AttemptRequest, error) {
	var r AttemptRequest
	if err := decodeAttempt(raw, &r, []string{"workload_identity", "execution_token", "manifest_id", "manifest_digest"}); err != nil {
		return r, err
	}
	return r, r.Validate()
}
func DecodeAttemptResponse(raw []byte) (AttemptResponse, error) {
	var r AttemptResponse
	if err := decodeAttempt(raw, &r, []string{"tenant_id", "profile_id", "profile_revision_number", "run_id", "attempt_id", "worker_id", "lease_epoch", "expires_at", "manifest_id", "manifest_digest", "allowed_uses"}); err != nil {
		return r, err
	}
	return r, r.Validate()
}
func decodeAttempt(raw []byte, target any, keys []string) error {
	if len(raw) == 0 || len(raw) > MaxAttemptProofBytes || !utf8.Valid(raw) {
		return ErrInvalidAttemptProof
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return ErrInvalidAttemptProof
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(canonical, &fields) != nil || len(fields) != len(keys) {
		return ErrInvalidAttemptProof
	}
	for _, k := range keys {
		if v, ok := fields[k]; !ok || bytes.Equal(v, []byte("null")) {
			return ErrInvalidAttemptProof
		}
	}
	if uses, ok := fields["allowed_uses"]; ok {
		var entries []map[string]json.RawMessage
		if json.Unmarshal(uses, &entries) != nil {
			return ErrInvalidAttemptProof
		}
		for _, entry := range entries {
			if len(entry) != 3 {
				return ErrInvalidAttemptProof
			}
			for _, k := range []string{"credential_id", "purpose", "audience_digest"} {
				v, ok := entry[k]
				if !ok || bytes.Equal(v, []byte("null")) {
					return ErrInvalidAttemptProof
				}
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return ErrInvalidAttemptProof
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalidAttemptProof
	}
	return nil
}
