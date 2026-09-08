package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func telegramMedia(m *telegramMessage, version int64) *channels.MediaReference {
	if d := m.Document; d != nil && d.FileID != "" {
		return &channels.MediaReference{FileID: d.FileID, Name: d.FileName, DeclaredMIME: d.MimeType, Size: d.FileSize, BindingVersion: version}
	}
	if len(m.Photo) > 0 {
		p := m.Photo[len(m.Photo)-1]
		if p.FileID != "" {
			return &channels.MediaReference{FileID: p.FileID, DeclaredMIME: "image/jpeg", Size: p.FileSize, BindingVersion: version}
		}
	}
	return nil
}

// DownloadMedia accepts only a provider file_id, never a user URL. Alternate
// endpoints require an injected test client, not a tenant JSON setting.
func (a *Adapter) DownloadMedia(parent context.Context, b controlplane.ChannelBinding, ref channels.MediaReference, maxBytes int64) ([]byte, error) {
	cfg, err := parseBinding(b)
	if err != nil {
		return nil, err
	}
	if !cfg.AttachmentsEnabled || b.Version != ref.BindingVersion || ref.FileID == "" || len(ref.FileID) > 1024 || maxBytes <= 0 || ref.Size > maxBytes {
		return nil, errors.New("attachment binding or size rejected")
	}
	base := apiBase(cfg)
	if !a.customClient && base != defaultAPIBase {
		return nil, errors.New("attachment endpoint must be the official Telegram API")
	}
	token, err := a.secrets.Resolve(parent, b.TenantID, secret.TelegramMedia, cfg.BotTokenRef)
	if err != nil {
		return nil, secret.ErrForbidden
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	client := *a.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("attachment redirects disabled") }
	body, _ := json.Marshal(map[string]string{"file_id": ref.FileID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/bot"+url.PathEscape(token)+"/getFile", bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("cannot construct media request")
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("attachment metadata unavailable")
	}
	var meta struct {
		OK     bool `json:"ok"`
		Result struct {
			Path string `json:"file_path"`
			Size int64  `json:"file_size"`
		} `json:"result"`
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&meta)
	_ = response.Body.Close()
	if err != nil || !meta.OK || response.StatusCode != 200 || meta.Result.Size > maxBytes {
		return nil, errors.New("attachment metadata rejected")
	}
	p := meta.Result.Path
	if p == "" || len(p) > 1024 || strings.HasPrefix(p, "/") || strings.ContainsAny(p, "\\?#\x00") || path.Clean(p) != p {
		return nil, errors.New("unsafe provider file path")
	}
	segments := strings.Split(p, "/")
	for i, v := range segments {
		if v == ".." || v == "." || v == "" {
			return nil, errors.New("unsafe provider file path")
		}
		segments[i] = url.PathEscape(v)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, base+"/file/bot"+url.PathEscape(token)+"/"+strings.Join(segments, "/"), nil)
	if err != nil {
		return nil, errors.New("cannot construct file download")
	}
	response, err = client.Do(req)
	if err != nil {
		return nil, errors.New("attachment download unavailable")
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(response.Body)
	if response.StatusCode != 200 || response.ContentLength > maxBytes {
		return nil, errors.New("attachment download rejected")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil || int64(len(data)) > maxBytes {
		return nil, errors.New("attachment download exceeds bound or failed")
	}
	return data, nil
}
