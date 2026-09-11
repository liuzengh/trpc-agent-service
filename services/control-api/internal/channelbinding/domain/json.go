package domain

import (
	"github.com/gowebpki/jcs"
	"unicode/utf8"
)

func canonicalRaw(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, failure(SourceIntegrity, "")
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, failure(SourceIntegrity, "")
	}
	return canonical, nil
}
