package application

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/gowebpki/jcs"
	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	controlruntimev1 "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	"io"
	"strings"
	"time"
)

var ErrManifestExportCursor = errors.New("invalid manifest export cursor")

type ManifestExportRow struct {
	Position controlruntimev1.ExportPosition `json:"position"`
	Payload  json.RawMessage                 `json:"payload"`
	Digest   string                          `json:"digest"`
}
type ManifestExportStore interface {
	ManifestExportUpper(context.Context) (*controlruntimev1.ExportPosition, error)
	ManifestExportRows(context.Context, *controlruntimev1.ExportPosition, *controlruntimev1.ExportPosition, int) ([]ManifestExportRow, error)
}
type exportCursor struct {
	Version string                          `json:"version"`
	Upper   controlruntimev1.ExportPosition `json:"upper"`
	After   controlruntimev1.ExportPosition `json:"after"`
}

func positionValid(p controlruntimev1.ExportPosition) bool {
	return !p.CreatedAt.IsZero() && p.CreatedAt.After(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) && len(p.TenantID) > 0 && len(p.TenantID) <= 128 && len(p.EventID) > 0 && len(p.EventID) <= 128
}
func positionCompare(a, b controlruntimev1.ExportPosition) int {
	if a.CreatedAt.Before(b.CreatedAt) {
		return -1
	}
	if a.CreatedAt.After(b.CreatedAt) {
		return 1
	}
	if a.TenantID < b.TenantID {
		return -1
	}
	if a.TenantID > b.TenantID {
		return 1
	}
	if a.EventID < b.EventID {
		return -1
	}
	if a.EventID > b.EventID {
		return 1
	}
	return 0
}

// ExportManifestPage is a bounded keyset export. A Worker creates the retained
// event durable before its initial call and consumes that durable afterward,
// covering transactions that commit below a cursor while export is in progress.
func ExportManifestPage(ctx context.Context, store ManifestExportStore, cursor string, limit int, cursorKey []byte) (controlruntimev1.ManifestExportPage, error) {
	page := controlruntimev1.ManifestExportPage{SchemaVersion: "v1", Events: []json.RawMessage{}}
	if store == nil || len(cursorKey) < 32 {
		return page, ErrManifestDistributionUnavailable
	}
	if limit < 1 || limit > controlruntimev1.MaxExportPageSize {
		return page, ErrManifestExportCursor
	}
	var after *controlruntimev1.ExportPosition
	if cursor == "" {
		upper, err := store.ManifestExportUpper(ctx)
		if err != nil {
			return page, err
		}
		page.SnapshotUpper = upper
	} else {
		if len(cursor) > 2048 {
			return page, ErrManifestExportCursor
		}
		parts := strings.Split(cursor, ".")
		if len(parts) != 2 {
			return page, ErrManifestExportCursor
		}
		signature, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
		if err != nil || !hmac.Equal(signature, exportCursorMAC(cursorKey, parts[0])) {
			return page, ErrManifestExportCursor
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
		if err != nil {
			return page, ErrManifestExportCursor
		}
		canonical, err := jcs.Transform(raw)
		if err != nil {
			return page, ErrManifestExportCursor
		}
		var c exportCursor
		dec := json.NewDecoder(bytes.NewReader(canonical))
		dec.DisallowUnknownFields()
		if dec.Decode(&c) != nil || c.Version != "v1" || !positionValid(c.Upper) || !positionValid(c.After) || positionCompare(c.After, c.Upper) > 0 {
			return page, ErrManifestExportCursor
		}
		if err = dec.Decode(new(any)); err != io.EOF {
			return page, ErrManifestExportCursor
		}
		page.SnapshotUpper = &c.Upper
		after = &c.After
	}
	if page.SnapshotUpper == nil {
		page.Complete = true
		return page, nil
	}
	if !positionValid(*page.SnapshotUpper) {
		return page, ErrManifestDistributionUnavailable
	}
	rows, err := store.ManifestExportRows(ctx, after, page.SnapshotUpper, limit+1)
	if err != nil {
		return page, err
	}
	page.Complete = len(rows) <= limit
	if !page.Complete {
		rows = rows[:limit]
	}
	var previous = after
	for _, row := range rows {
		if !positionValid(row.Position) || positionCompare(row.Position, *page.SnapshotUpper) > 0 || (previous != nil && positionCompare(row.Position, *previous) <= 0) {
			return controlruntimev1.ManifestExportPage{}, ErrManifestDistributionUnavailable
		}
		e, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(row.Payload)
		if err != nil {
			return controlruntimev1.ManifestExportPage{}, ErrManifestDistributionUnavailable
		}
		digest, err := controleventsv1.ManifestEventDigest(row.Payload)
		if err != nil || digest != row.Digest || e.EventID != row.Position.EventID || e.TenantID != row.Position.TenantID {
			return controlruntimev1.ManifestExportPage{}, ErrManifestDistributionUnavailable
		}
		page.Events = append(page.Events, row.Payload)
		p := row.Position
		previous = &p
	}
	if !page.Complete {
		c := exportCursor{Version: "v1", Upper: *page.SnapshotUpper, After: rows[len(rows)-1].Position}
		raw, _ := json.Marshal(c)
		payload := base64.RawURLEncoding.EncodeToString(raw)
		page.NextCursor = payload + "." + base64.RawURLEncoding.EncodeToString(exportCursorMAC(cursorKey, payload))
	}
	return page, nil
}

func exportCursorMAC(key []byte, payload string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("manifest-export-cursor-v1\x00"))
	mac.Write([]byte(payload))
	return mac.Sum(nil)
}
