package domain

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func TestAttachmentValidation(t *testing.T) {
	good := Attachment{Name: "报告.txt", Version: 0, MimeType: "text/plain; charset=utf-8", SizeBytes: 0, SHA256: strings.Repeat("a", 64)}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", " ", ".", "..", "../a", "a/b", "a\\b", "a\x00b", "a\nb", " spaced", "spaced ", strings.Repeat("a", 256), string([]byte{255})} {
		a := good
		a.Name = name
		if a.Validate() == nil {
			t.Fatalf("accepted invalid name %q", name)
		}
	}
	for _, mutate := range []func(*Attachment){func(a *Attachment) { a.Version = -1 }, func(a *Attachment) { a.SizeBytes = -1 }, func(a *Attachment) { a.MimeType = "" }, func(a *Attachment) { a.MimeType = "text/" + strings.Repeat("a", 251) }, func(a *Attachment) { a.MimeType = "text/plain\r\nX: value" }, func(a *Attachment) { a.SHA256 = strings.Repeat("A", 64) }, func(a *Attachment) { a.SHA256 = "sha256:" + a.SHA256 }, func(a *Attachment) { a.SHA256 = strings.Repeat("z", 64) }} {
		a := good
		mutate(&a)
		if a.Validate() == nil {
			t.Fatalf("accepted invalid metadata %+v", a)
		}
	}
	if strconv.IntSize == 64 {
		max := int64(9007199254740991)
		a := good
		a.Version = int(max)
		a.SizeBytes = int(max)
		if e := a.Validate(); e != nil {
			t.Fatal("wire exact integer rejected", e)
		}
		a.Version = int(max + 1)
		if a.Validate() == nil {
			t.Fatal("inexact version accepted")
		}
		a.Version = 0
		a.SizeBytes = int(max + 1)
		if a.Validate() == nil {
			t.Fatal("inexact size accepted")
		}
	}
	if ValidateAttachments([]Attachment{good, good}) == nil {
		t.Fatal("duplicate accepted")
	}
	next := good
	next.Version = 1
	if err := ValidateAttachments([]Attachment{good, next}); err != nil {
		t.Fatal(err)
	}
}

func TestFinishAttachmentDigestPreservesLegacyAndSealsAllMetadata(t *testing.T) {
	f := Finish{Grant: Grant{AttemptID: "attempt", Generation: 1, LeaseEpoch: 2}, Status: Succeeded, Candidate: Candidate{Ref: "candidate", Digest: Digest([]byte("candidate"))}, FinalText: "final"}
	for _, memory := range []string{"", Digest([]byte("memory"))} {
		f.MemoryDigest = memory
		legacy := struct {
			Attempt           string
			Generation, Epoch int64
			Status            Status
			Candidate         Candidate
			Text, Reason      string
			MemoryDigest      string `json:",omitempty"`
		}{f.Grant.AttemptID, f.Grant.Generation, f.Grant.LeaseEpoch, f.Status, f.Candidate, f.FinalText, f.Reason, f.MemoryDigest}
		raw, _ := json.Marshal(legacy)
		f.Attachments = nil
		if got := FinishDigest(f); got != Digest(raw) {
			t.Fatalf("legacy nil digest changed %s", got)
		}
		f.Attachments = []Attachment{}
		if got := FinishDigest(f); got != Digest(raw) {
			t.Fatalf("legacy empty digest changed %s", got)
		}
	}
	f.Attachments = []Attachment{{Name: "report.txt", Version: 0, MimeType: "text/plain", SizeBytes: 7, SHA256: strings.Repeat("a", 64)}}
	digest := FinishDigest(f)
	for _, mutate := range []func(*Attachment){func(a *Attachment) { a.Name = "other.txt" }, func(a *Attachment) { a.Version++ }, func(a *Attachment) { a.MimeType = "text/csv" }, func(a *Attachment) { a.SizeBytes++ }, func(a *Attachment) { a.SHA256 = strings.Repeat("b", 64) }} {
		next := f
		next.Attachments = append([]Attachment(nil), f.Attachments...)
		mutate(&next.Attachments[0])
		if FinishDigest(next) == digest {
			t.Fatal("changed attachment did not affect result digest")
		}
	}
}

func TestAttachmentMIMEUnicodeWireBoundary(t *testing.T) {
	mime := "text/plain;x=\"" + strings.Repeat("a", 239) + "é\""
	a := Attachment{Name: "report.txt", Version: 0, MimeType: mime, SizeBytes: 1, SHA256: strings.Repeat("a", 64)}
	if len(mime) != 256 || a.Validate() != nil {
		t.Fatal("valid 256-byte / 255-character MIME rejected")
	}
}
