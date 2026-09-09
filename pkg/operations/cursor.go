package operations

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"
)

type position struct {
	Version int       `json:"v"`
	View    string    `json:"view"`
	Tenant  string    `json:"tenant"`
	Status  string    `json:"status"`
	Time    time.Time `json:"time"`
	ID      string    `json:"id"`
}

func validID(id string, numeric bool) bool {
	if numeric {
		n, err := strconv.ParseInt(id, 10, 64)
		return err == nil && n > 0 && strconv.FormatInt(n, 10) == id
	}
	return len(id) > 0 && len(id) <= 128 && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\x00\r\n")
}

func (q *query) parseCursor(view string) error {
	if q.cursor == "" {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(q.cursor)
	if err != nil {
		return errQuery
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var p position
	if err = decoder.Decode(&p); err != nil {
		return errQuery
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return errQuery
	}
	spec, ok := views[view]
	if !ok || p.Version != 1 || p.View != view || p.Tenant != q.tenant || p.Status != q.status || p.Time.IsZero() || !validID(p.ID, spec.numeric) {
		return errQuery
	}
	q.after = &p
	return nil
}
func cursorFor(view string, q query, t time.Time, id string) string {
	data, _ := json.Marshal(position{Version: 1, View: view, Tenant: q.tenant, Status: q.status, Time: t.UTC(), ID: id})
	return base64.RawURLEncoding.EncodeToString(data)
}
