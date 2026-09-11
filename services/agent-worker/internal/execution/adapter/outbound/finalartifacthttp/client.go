// Package finalartifacthttp resolves only the two published Artifact credentials
// against committed Final evidence. It never submits an active Attempt token.
package finalartifacthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

const ResolvePath = "/internal/v1/runtime-profiles/credentials/resolve-final-artifact"

var ErrDenied = errors.New("committed Artifact credential authorization denied")
var ErrUnavailable = errors.New("committed Artifact credential dependency unavailable")
var identity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type Request struct {
	Final                 proof.FinalRequest `json:"final"`
	TenantID              string             `json:"tenant_id"`
	ManifestID            string             `json:"manifest_id"`
	ManifestDigest        string             `json:"manifest_digest"`
	DeploymentID          string             `json:"deployment_id"`
	DeploymentRevisionID  string             `json:"deployment_revision_id"`
	ProfileID             string             `json:"profile_id"`
	ProfileRevisionNumber int64              `json:"profile_revision_number"`
}
type Options struct {
	BaseURL, WorkerID string
	Client            *http.Client
	Timeout           time.Duration
	MaxResponseBytes  int64
}
type Client struct {
	base, worker string
	client       *http.Client
	timeout      time.Duration
	maxBytes     int64
}

func New(o Options) (*Client, error) {
	u, e := url.Parse(o.BaseURL)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || o.WorkerID == "" || o.Client == nil || o.Timeout <= 0 || o.MaxResponseBytes < 1 {
		return nil, ErrDenied
	}
	client := *o.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimRight(o.BaseURL, "/"), worker: o.WorkerID, client: &client, timeout: o.Timeout, maxBytes: o.MaxResponseBytes}, nil
}
func (r Request) Validate() error {
	if r.Final.Validate() != nil || !domain.DigestValid(r.ManifestDigest) || r.ProfileRevisionNumber < 1 || r.ProfileRevisionNumber > 9007199254740991 {
		return ErrDenied
	}
	for _, id := range []string{r.TenantID, r.ManifestID, r.DeploymentID, r.DeploymentRevisionID, r.ProfileID} {
		if !identity.MatchString(id) {
			return ErrDenied
		}
	}
	return nil
}
func (c *Client) Resolve(ctx context.Context, r Request, uses []domain.CredentialUse) (artifactstore.Credentials, error) {
	var zero artifactstore.Credentials
	if r.Validate() != nil || len(uses) != 2 || uses[0].Purpose != "access_key_id" || uses[1].Purpose != "secret_access_key" || uses[0].CredentialID == "" || uses[1].CredentialID == "" || uses[0].CredentialID == uses[1].CredentialID || !domain.DigestValid(uses[0].AudienceDigest) || uses[0].AudienceDigest != uses[1].AudienceDigest {
		return zero, ErrDenied
	}
	body, err := json.Marshal(r)
	if err != nil {
		return zero, ErrDenied
	}
	defer clear(body)
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+ResolvePath, bytes.NewReader(body))
	if err != nil {
		return zero, ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return zero, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		if response.StatusCode == 429 || response.StatusCode >= 500 {
			return zero, ErrUnavailable
		}
		return zero, ErrDenied
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return zero, ErrDenied
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, c.maxBytes+1))
	defer clear(raw)
	if err != nil {
		return zero, ErrUnavailable
	}
	if int64(len(raw)) > c.maxBytes {
		return zero, ErrDenied
	}
	batch, err := decodeBatch(raw)
	if err != nil {
		return zero, ErrDenied
	}
	defer func() {
		for i := range batch.Credentials {
			batch.Credentials[i].Value = ""
		}
	}()
	if batch.TenantID != r.TenantID || batch.ProfileID != r.ProfileID || batch.ProfileRevision != r.ProfileRevisionNumber || batch.RunID != r.Final.RunID || batch.AttemptID != r.Final.AttemptID || batch.WorkerID != c.worker || batch.LeaseEpoch != 0 || batch.ManifestID != r.ManifestID || batch.ManifestDigest != r.ManifestDigest {
		return zero, ErrDenied
	}
	expected := map[domain.CredentialUse]bool{uses[0]: true, uses[1]: true}
	values := map[string]string{}
	for _, item := range batch.Credentials {
		use := domain.CredentialUse{CredentialID: item.CredentialID, Purpose: item.Purpose, AudienceDigest: item.AudienceDigest}
		if !expected[use] || item.Revision < 1 || item.Revision > 9007199254740991 || strings.TrimSpace(item.Value) == "" || !utf8.ValidString(item.Value) || strings.ContainsAny(item.Value, "\x00\r\n") {
			return zero, ErrDenied
		}
		delete(expected, use)
		values[use.Purpose] = item.Value
	}
	if len(expected) != 0 {
		return zero, ErrDenied
	}
	return artifactstore.Credentials{AccessKeyID: values["access_key_id"], SecretAccessKey: values["secret_access_key"]}, nil
}

type credential struct {
	CredentialID   string `json:"credential_id"`
	Purpose        string `json:"purpose"`
	AudienceDigest string `json:"audience_digest"`
	Revision       int64  `json:"credential_revision"`
	Value          string `json:"value"`
}
type batch struct {
	TenantID        string       `json:"tenant_id"`
	ProfileID       string       `json:"profile_id"`
	ProfileRevision int64        `json:"profile_revision_number"`
	RunID           string       `json:"run_id"`
	AttemptID       string       `json:"attempt_id"`
	WorkerID        string       `json:"worker_id"`
	LeaseEpoch      int64        `json:"lease_epoch"`
	ManifestID      string       `json:"manifest_id"`
	ManifestDigest  string       `json:"manifest_digest"`
	Credentials     []credential `json:"credentials"`
}

func closedObject(raw []byte, keys []string) error {
	if !utf8.Valid(raw) {
		return ErrDenied
	}
	expected := map[string]bool{}
	for _, k := range keys {
		expected[k] = true
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	t, e := d.Token()
	if e != nil || t != json.Delim('{') {
		return ErrDenied
	}
	for d.More() {
		t, e = d.Token()
		k, ok := t.(string)
		if e != nil || !ok || !expected[k] {
			return ErrDenied
		}
		delete(expected, k)
		var v json.RawMessage
		if d.Decode(&v) != nil || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return ErrDenied
		}
	}
	if t, e = d.Token(); e != nil || t != json.Delim('}') || len(expected) != 0 {
		return ErrDenied
	}
	if _, e = d.Token(); e != io.EOF {
		return ErrDenied
	}
	return nil
}
func decodeBatch(raw []byte) (batch, error) {
	var out batch
	if closedObject(raw, []string{"tenant_id", "profile_id", "profile_revision_number", "run_id", "attempt_id", "worker_id", "lease_epoch", "manifest_id", "manifest_digest", "credentials"}) != nil {
		return out, ErrDenied
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return out, ErrDenied
	}
	var items []json.RawMessage
	if json.Unmarshal(fields["credentials"], &items) != nil || len(items) != 2 {
		return out, ErrDenied
	}
	for _, item := range items {
		if closedObject(item, []string{"credential_id", "purpose", "audience_digest", "credential_revision", "value"}) != nil {
			return out, ErrDenied
		}
	}
	if json.Unmarshal(raw, &out) != nil {
		return batch{}, ErrDenied
	}
	return out, nil
}
