package configfile

import (
	"encoding/json"
	"github.com/gowebpki/jcs"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
	"reflect"
	"strings"
	"unicode/utf8"
)

type RuntimeDocument struct {
	Version  string                 `json:"version"`
	Backends []domain.RuntimeTarget `json:"backends"`
}

// LoadRuntime loads owner-only, release-pinned physical targets. An absent pair
// leaves managed compilation unconfigured; it never manufactures default targets.
func LoadRuntime(path, expected string, directory *domain.Catalog) (*domain.RuntimeCatalog, error) {
	if path == "" && expected == "" {
		return nil, nil
	}
	b, err := readPinned(path, expected, true)
	if err != nil {
		return nil, ErrConfig
	}
	return DecodeRuntime(b, directory)
}
func DecodeRuntime(b []byte, directory *domain.Catalog) (*domain.RuntimeCatalog, error) {
	if len(b) == 0 || len(b) > MaxBytes || !utf8.Valid(b) {
		return nil, ErrConfig
	}
	if _, err := jcs.Transform(b); err != nil {
		return nil, ErrConfig
	}
	var tree any
	if json.Unmarshal(b, &tree) != nil || !exactRuntimeFields(tree, reflect.TypeFor[RuntimeDocument]()) {
		return nil, ErrConfig
	}
	var doc RuntimeDocument
	if json.Unmarshal(b, &doc) != nil || doc.Version != "v1" {
		return nil, ErrConfig
	}
	result, err := domain.NewRuntimeCatalog(directory, doc.Backends)
	if err != nil {
		return nil, ErrConfig
	}
	return result, nil
}
func exactRuntimeFields(v any, t reflect.Type) bool {
	if v == nil {
		return false
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
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
			if !ok || !exactRuntimeFields(v, f.Type) {
				return false
			}
		}
	case reflect.Slice:
		xs, ok := v.([]any)
		if !ok {
			return false
		}
		for _, v := range xs {
			if !exactRuntimeFields(v, t.Elem()) {
				return false
			}
		}
	}
	return true
}
