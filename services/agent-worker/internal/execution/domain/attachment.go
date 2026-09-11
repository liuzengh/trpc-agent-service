package domain

import (
	"encoding/hex"
	"mime"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Attachment seals a selected immutable Artifact version in the accepted Final.
// It contains no storage address or credential and grants no access by itself.
type Attachment struct {
	Name      string
	Version   int
	MimeType  string
	SizeBytes int
	SHA256    string
}

type AcceptedArtifact struct {
	Run        Run
	Final      Final
	Attachment Attachment
}

func (a Attachment) Validate() error {
	if !utf8.ValidString(a.Name) || strings.TrimSpace(a.Name) == "" || strings.TrimSpace(a.Name) != a.Name || len(a.Name) > 255 || a.Name == "." || a.Name == ".." || strings.ContainsAny(a.Name, "/\\") || strings.IndexFunc(a.Name, unicode.IsControl) >= 0 || a.Version < 0 || a.SizeBytes < 0 || int64(a.Version) > 9007199254740991 || int64(a.SizeBytes) > 9007199254740991 {
		return ErrInvalid
	}
	if len(a.MimeType) > 256 || utf8.RuneCountInString(a.MimeType) > 255 || !utf8.ValidString(a.MimeType) || strings.IndexFunc(a.MimeType, unicode.IsControl) >= 0 {
		return ErrInvalid
	}
	if _, _, err := mime.ParseMediaType(a.MimeType); err != nil || !strings.Contains(a.MimeType, "/") {
		return ErrInvalid
	}
	sum, err := hex.DecodeString(a.SHA256)
	if err != nil || len(sum) != 32 || len(a.SHA256) != 64 || strings.ToLower(a.SHA256) != a.SHA256 {
		return ErrInvalid
	}
	return nil
}

func ValidateAttachments(items []Attachment) error {
	type identity struct {
		name    string
		version int
	}
	seen := make(map[identity]bool, len(items))
	for _, a := range items {
		if err := a.Validate(); err != nil {
			return err
		}
		key := identity{a.Name, a.Version}
		if seen[key] {
			return ErrInvalid
		}
		seen[key] = true
	}
	return nil
}
