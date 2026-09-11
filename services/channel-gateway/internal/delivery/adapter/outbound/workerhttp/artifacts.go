package workerhttp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"io"
	"net/http"
)

// ReadReplyArtifact uses the same authenticated Worker origin as committed Final
// verification. No caller-provided locator or credentials cross this boundary.
func (c *Client) ReadReplyArtifact(ctx context.Context, i d.Intent, a d.Attachment) ([]byte, error) {
	if ctx == nil || i.Validate() != nil {
		return nil, d.ErrInvalid
	}
	found := false
	for _, item := range i.Attachments {
		if item == a {
			found = true
			break
		}
	}
	if !found {
		return nil, d.ErrUnauthorized
	}
	raw, _ := json.Marshal(struct {
		IntentID     string `json:"intent_id"`
		RunID        string `json:"run_id"`
		CompletionID string `json:"completion_id"`
		Name         string `json:"name"`
		Version      int64  `json:"version"`
	}{i.ID, i.RunID, i.CompletionID, a.Name, a.Version})
	u := *c.base
	u.Path = "/internal/v1/reply-artifacts"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(raw))
	if err != nil {
		return nil, d.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	tracecontext.Capture(ctx).Inject(req.Header)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, d.ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusConflict || res.StatusCode == http.StatusForbidden {
		return nil, d.ErrUnauthorized
	}
	if res.StatusCode != http.StatusOK {
		return nil, d.ErrUnavailable
	}
	if res.ContentLength != a.SizeBytes || res.Header.Get("Content-Type") != a.MIMEType || res.Header.Get("X-Content-SHA256") != a.SHA256 || (res.Header.Get("Content-Encoding") != "" && res.Header.Get("Content-Encoding") != "identity") {
		return nil, d.ErrInvalid
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, a.SizeBytes+1))
	if err != nil {
		return nil, d.ErrUnavailable
	}
	hash := sha256.Sum256(body)
	if int64(len(body)) != a.SizeBytes || hex.EncodeToString(hash[:]) != a.SHA256 {
		return nil, d.ErrInvalid
	}
	return body, nil
}
