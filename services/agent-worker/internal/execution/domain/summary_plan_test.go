package domain

import "testing"

func TestPlanSummaryCredentialClosure(t *testing.T) {
	main := CredentialUse{CredentialID: "main", Purpose: "api_key", AudienceDigest: Digest([]byte("main"))}
	session := CredentialUse{CredentialID: "session", Purpose: "dsn", AudienceDigest: Digest([]byte("session"))}
	p := Plan{ModelCredential: main, SessionCredential: session}
	if uses := p.Uses(); len(uses) != 2 || uses[0] != main || uses[1] != session {
		t.Fatal("legacy order changed")
	}
	p.Summary = &SummaryPlan{ModelCredential: main}
	if uses := p.Uses(); len(uses) != 2 {
		t.Fatal("shared model credentials duplicated")
	}
	summary := CredentialUse{CredentialID: "summary", Purpose: "api_key", AudienceDigest: Digest([]byte("summary"))}
	p.Summary.ModelCredential = summary
	if uses := p.Uses(); len(uses) != 3 || uses[2] != summary {
		t.Fatal("summary missing from execution proof closure")
	}
	p.Summary.ModelCredential = CredentialUse{}
	if uses := p.Uses(); len(uses) != 2 {
		t.Fatal("anonymous summary should not emit empty use")
	}
}
