package executionv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const ReplyArtifactPath = "/internal/v1/reply-artifacts"
const MaxReplyArtifactRequestBytes = 4096

var ErrInvalidReplyArtifact = errors.New("invalid accepted reply artifact request")

// ReplyArtifactRequest selects only a file already attached to a committed
// Final. Tenant, Session, backend, credentials and object paths are never input.
// Version is required on the wire, including the first version (zero).
type ReplyArtifactRequest struct {
	IntentID     string `json:"intent_id"`
	RunID        string `json:"run_id"`
	CompletionID string `json:"completion_id"`
	Name         string `json:"name"`
	Version      int    `json:"version"`
}

// ReplyArtifactResponse is an in-process result. The HTTP representation is raw
// Content with trusted metadata headers, never a JSON/base64 success envelope.
type ReplyArtifactResponse struct {
	Content   []byte
	MimeType  string
	SizeBytes int
	SHA256    string
}

func (r ReplyArtifactRequest) Validate() error {
	if !finalID.MatchString(r.IntentID) || !finalID.MatchString(r.RunID) || !finalID.MatchString(r.CompletionID) || r.Version < 0 || r.Name == "" || r.Name == "." || r.Name == ".." || len(r.Name) > 255 || !utf8.ValidString(r.Name) || strings.TrimSpace(r.Name) != r.Name || strings.ContainsAny(r.Name, "/\\\x00\r\n") || strings.IndexFunc(r.Name, unicode.IsControl) >= 0 {
		return ErrInvalidReplyArtifact
	}
	return nil
}
func EncodeReplyArtifactRequest(r ReplyArtifactRequest) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}
func DecodeReplyArtifactRequest(raw []byte) (ReplyArtifactRequest, error) {
	var r ReplyArtifactRequest
	if len(raw) == 0 || len(raw) > MaxReplyArtifactRequestBytes || !utf8.Valid(raw) {
		return r, ErrInvalidReplyArtifact
	}
	allowed := map[string]bool{"intent_id": true, "run_id": true, "completion_id": true, "name": true, "version": true}
	seen := map[string]bool{}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return r, ErrInvalidReplyArtifact
	}
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		if err != nil || !ok || !allowed[key] || seen[key] {
			return r, ErrInvalidReplyArtifact
		}
		seen[key] = true
		value, e := d.Token()
		if e != nil {
			return r, ErrInvalidReplyArtifact
		}
		if key == "version" {
			n, ok := value.(json.Number)
			if !ok {
				return r, ErrInvalidReplyArtifact
			}
			if _, e = n.Int64(); e != nil {
				return r, ErrInvalidReplyArtifact
			}
		} else if _, ok := value.(string); !ok {
			return r, ErrInvalidReplyArtifact
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') || len(seen) != len(allowed) {
		return r, ErrInvalidReplyArtifact
	}
	if _, err = d.Token(); err != io.EOF {
		return r, ErrInvalidReplyArtifact
	}
	if json.Unmarshal(raw, &r) != nil {
		return ReplyArtifactRequest{}, ErrInvalidReplyArtifact
	}
	if err = r.Validate(); err != nil {
		return ReplyArtifactRequest{}, err
	}
	return r, nil
}
