// Package executionv1 contains the internal Execution proof protocol. Final proof
// is immutable committed evidence and is deliberately separate from live leases.
package executionv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"unicode/utf8"
)

const FinalVerifyPath = "/internal/v1/execution/finals:verify"
const MaxFinalProofBytes = 4096

var ErrInvalidFinalProof = errors.New("invalid committed Final proof")
var finalID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var finalDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type FinalRequest struct {
	IntentID            string `json:"intent_id"`
	Digest              string `json:"digest"`
	AdmissionID         string `json:"admission_id"`
	RunID               string `json:"run_id"`
	AttemptID           string `json:"attempt_id"`
	CompletionID        string `json:"completion_id"`
	ExecutionGeneration int64  `json:"execution_generation"`
	Sequence            int64  `json:"sequence"`
}
type FinalResponse struct {
	FinalRequest
	TenantID       string `json:"tenant_id"`
	ManifestDigest string `json:"manifest_digest"`
}

func (r FinalRequest) Validate() error {
	for _, id := range []string{r.IntentID, r.AdmissionID, r.RunID, r.AttemptID, r.CompletionID} {
		if !finalID.MatchString(id) {
			return ErrInvalidFinalProof
		}
	}
	if !finalDigest.MatchString(r.Digest) || r.ExecutionGeneration < 1 || r.ExecutionGeneration > 9007199254740991 || r.Sequence != 1 {
		return ErrInvalidFinalProof
	}
	return nil
}
func (r FinalResponse) Validate() error {
	if r.FinalRequest.Validate() != nil || !finalID.MatchString(r.TenantID) || !finalDigest.MatchString(r.ManifestDigest) {
		return ErrInvalidFinalProof
	}
	return nil
}
func EncodeFinalRequest(r FinalRequest) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}
func EncodeFinalResponse(r FinalResponse) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}
func DecodeFinalRequest(raw []byte) (FinalRequest, error) {
	var r FinalRequest
	if err := decodeFinal(raw, &r, false); err != nil {
		return r, err
	}
	return r, r.Validate()
}
func DecodeFinalResponse(raw []byte) (FinalResponse, error) {
	var r FinalResponse
	if err := decodeFinal(raw, &r, true); err != nil {
		return r, err
	}
	return r, r.Validate()
}

// Token validation rejects duplicate, case-folded and unknown fields before the
// typed decoder, which otherwise permits those ambiguous encodings.
func decodeFinal(raw []byte, out any, response bool) error {
	if len(raw) == 0 || len(raw) > MaxFinalProofBytes || !utf8.Valid(raw) {
		return ErrInvalidFinalProof
	}
	allowed := map[string]bool{"intent_id": true, "digest": true, "admission_id": true, "run_id": true, "attempt_id": true, "completion_id": true, "execution_generation": true, "sequence": true}
	if response {
		allowed["tenant_id"] = true
		allowed["manifest_digest"] = true
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalidFinalProof
	}
	seen := map[string]bool{}
	for d.More() {
		k, e := d.Token()
		key, ok := k.(string)
		if e != nil || !ok || !allowed[key] || seen[key] {
			return ErrInvalidFinalProof
		}
		seen[key] = true
		value, e := d.Token()
		if e != nil {
			return ErrInvalidFinalProof
		}
		if key == "execution_generation" || key == "sequence" {
			n, ok := value.(json.Number)
			if !ok {
				return ErrInvalidFinalProof
			}
			if _, e = n.Int64(); e != nil {
				return ErrInvalidFinalProof
			}
		} else {
			if _, ok := value.(string); !ok {
				return ErrInvalidFinalProof
			}
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') || len(seen) != len(allowed) {
		return ErrInvalidFinalProof
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrInvalidFinalProof
	}
	if json.Unmarshal(raw, out) != nil {
		return ErrInvalidFinalProof
	}
	return nil
}
