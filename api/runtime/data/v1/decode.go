package datav1

import (
	"bytes"
	"encoding/json"
	"github.com/gowebpki/jcs"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

const MaxSnapshotBytes = 16 * 1024

// Decode rejects duplicates, unknown/case-variant/inactive fields, null and
// omitted required fields. All parser failures have the same non-secret error.
func Decode(b []byte) (Snapshot, error) {
	if len(b) == 0 || len(b) > MaxSnapshotBytes || !utf8.Valid(b) {
		return Snapshot{}, ErrSnapshot
	}
	if _, err := jcs.Transform(b); err != nil {
		return Snapshot{}, ErrSnapshot
	}
	var tree any
	if json.Unmarshal(b, &tree) != nil || !exact(tree, reflect.TypeFor[Snapshot]()) {
		return Snapshot{}, ErrSnapshot
	}
	var s Snapshot
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&s) != nil {
		return Snapshot{}, ErrSnapshot
	}
	if d.Decode(new(any)) != io.EOF {
		return Snapshot{}, ErrSnapshot
	}
	if err := s.Validate(); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}
func exact(v any, t reflect.Type) bool {
	if v == nil {
		return false
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	fields := map[string]reflect.StructField{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")
		fields[tag[0]] = f
		if len(tag) == 1 {
			if _, ok := m[tag[0]]; !ok {
				return false
			}
		}
	}
	for k, v := range m {
		f, ok := fields[k]
		if !ok || !exact(v, f.Type) {
			return false
		}
	}
	return true
}
