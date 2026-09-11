package domain

import (
	"encoding/json"
	"mime"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

var attachmentHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validateAttachments(items []Attachment) error {
	seen := map[string]map[int64]bool{}
	for _, a := range items {
		media, _, mimeErr := mime.ParseMediaType(a.MIMEType)
		if a.Name == "" || a.Name == "." || a.Name == ".." || len(a.Name) > 255 || !utf8.ValidString(a.Name) || strings.ContainsAny(a.Name, `/\`) || strings.IndexFunc(a.Name, unicode.IsControl) >= 0 || a.Version < 0 || a.Version > MaxExactInteger || a.SizeBytes < 0 || a.SizeBytes > MaxExactInteger || len(a.MIMEType) > 256 || utf8.RuneCountInString(a.MIMEType) > 255 || mimeErr != nil || !strings.Contains(media, "/") || strings.ContainsAny(a.MIMEType, "\r\n") || !attachmentHash.MatchString(a.SHA256) {
			return ErrInvalid
		}
		if seen[a.Name] == nil {
			seen[a.Name] = map[int64]bool{}
		}
		if seen[a.Name][a.Version] {
			return ErrInvalid
		}
		seen[a.Name][a.Version] = true
	}
	return nil
}

// Plan appends immutable attachment descriptors after existing text parts. The
// part index, not its body syntax, distinguishes documents from text. Each part
// uses the existing A1/A2/result ledger and independently records its outcome.
func Plan(t Target, i Intent) ([]string, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	parts, err := PlanText(t, i.Text)
	if err != nil {
		return nil, err
	}
	if len(i.Attachments) > 0 && t.Provider != "telegram" {
		return nil, ErrUnsupported
	}
	// Match the existing gateway_delivery_intents.part_count SQL constraint.
	// This is a delivery storage boundary, not a new Worker attachment policy.
	if len(parts)+len(i.Attachments) > 64 {
		return nil, ErrCapacity
	}
	for _, a := range i.Attachments {
		b, _ := json.Marshal(a)
		parts = append(parts, string(b))
	}
	return parts, nil
}
func AttachmentAt(t Target, i Intent, index int) (Attachment, bool) {
	text, err := PlanText(t, i.Text)
	if err != nil {
		return Attachment{}, false
	}
	n := index - len(text)
	if n < 0 || n >= len(i.Attachments) {
		return Attachment{}, false
	}
	return i.Attachments[n], true
}
