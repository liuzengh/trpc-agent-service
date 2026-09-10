package mysql

import (
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelMySQLCandidateHelpers(t *testing.T) {
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "helper")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	candidate := channels.CandidateBindingContext{
		Channel: channels.ChannelTelegram, PublicRouteKeyDigest: routeDigest, BindingVersion: 1,
		ConfigDigest: strings.Repeat("a", 64), Purpose: channels.PurposeWebhookVerification,
		CandidateToken: "candidate-token", IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if !sameCandidate(candidate, candidate.Clone()) {
		t.Fatal("candidate clone did not compare equal")
	}
	changed := candidate
	changed.ConfigDigest = strings.Repeat("b", 64)
	if sameCandidate(candidate, changed) {
		t.Fatal("different candidate compared equal")
	}
	if token, err := newCandidateToken(); err != nil || token == "" {
		t.Fatalf("candidate token = %q, err=%v", token, err)
	}
	repo := NewRepository(nil)
	repo.candidates["expired"] = candidateRecord{context: channels.CandidateBindingContext{CandidateToken: "expired", ExpiresAt: time.Now().UTC().Add(-time.Second)}}
	if repo.candidateCount() != 0 {
		t.Fatal("expired candidate was retained")
	}
}
