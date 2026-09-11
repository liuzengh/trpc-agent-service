package postgresadapter_test

import (
	"context"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"strings"
	"testing"
)

func TestAttachmentPartsPersistAndFailIndependently(t *testing.T) {
	s, other, _ := ledgers(t, pg.Options{})
	ctx := context.Background()
	p := prepared("attachment-intent")
	p.Intent.Attachments = []d.Attachment{{Name: "one.bin", MIMEType: "application/octet-stream", SHA256: strings.Repeat("a", 64)}, {Name: "two.bin", Version: 1, MIMEType: "application/octet-stream", SHA256: strings.Repeat("b", 64)}}
	var err error
	p.Digest, err = d.IntentDigest(p.Intent)
	if err != nil {
		t.Fatal(err)
	}
	p.Parts, err = d.Plan(p.Target, p.Intent)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.Accept(ctx, p)
	if err != nil || receipt.PartCount != 3 {
		t.Fatal(receipt, err)
	}
	repeat, err := other.Accept(ctx, p)
	if err != nil || repeat != receipt {
		t.Fatal("replay", repeat, err)
	}
	text := oneClaim(t, s, claimRequest())
	if text.Part.Index != 0 {
		t.Fatal(text)
	}
	attempt := calling(t, s, text)
	if err = s.Finish(ctx, attempt, d.Result{Certainty: d.CertaintyAccepted, ProviderMessageID: "10"}); err != nil {
		t.Fatal(err)
	}
	file := oneClaim(t, other, claimRequest())
	if file.Part.Index != 1 || len(file.Intent.Attachments) != 2 || file.Intent.Attachments[0].Version != 0 {
		t.Fatal("lost descriptor", file)
	}
	fa := calling(t, other, file)
	if err = other.Finish(ctx, fa, d.Result{Certainty: d.CertaintyRejected, ErrorClass: d.ErrorPermanent}); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimDue(ctx, claimRequest())
	if err != nil || len(next) != 0 {
		t.Fatal("continued after rejected attachment", next, err)
	}
	snap, err := other.Get(ctx, p.Intent.ID)
	if err != nil || len(snap.Parts) != 3 || snap.Parts[0].State != d.Accepted || snap.Parts[1].State != d.Rejected || snap.Parts[2].State == d.Accepted {
		t.Fatal("false all-delivered", snap, err)
	}
}
