package fence

import (
	"context"
	"errors"
	"testing"
)

func TestTokenContextValidation(t *testing.T) {
	token := Token{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a",
		ExecutionID: 7, Generation: 3, Value: "token-a",
	}
	ctx := WithToken(context.Background(), token)
	got, err := TokenFromContext(ctx)
	if err != nil || got != token {
		t.Fatalf("token=%#v err=%v", got, err)
	}
	if _, err := TokenFromContext(context.Background()); !errors.Is(err, ErrTokenRequired) {
		t.Fatalf("missing token error=%v", err)
	}
	bad := token
	bad.Generation = 0
	if _, err := TokenFromContext(WithToken(context.Background(), bad)); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("invalid token error=%v", err)
	}
}

func TestTokenContextAllowsLegacyEmptySessionOwner(t *testing.T) {
	token := Token{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a",
		UserID: "user-a", ExecutionID: 7, Generation: 3, Value: "token-a",
	}
	got, err := TokenFromContext(WithToken(context.Background(), token))
	if err != nil || got != token {
		t.Fatalf("legacy token=%#v err=%v", got, err)
	}
}

func TestTokenContextRejectsInvalidSessionOwner(t *testing.T) {
	token := Token{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a",
		SessionOwnerID: "owner\x00", UserID: "user-a", ExecutionID: 7,
		Generation: 3, Value: "token-a",
	}
	if _, err := TokenFromContext(WithToken(context.Background(), token)); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("invalid session owner error=%v, want ErrInvalidToken", err)
	}
}

func TestScopeIsInjectiveForSeparators(t *testing.T) {
	if ScopeFor("ab", "c", "d") == ScopeFor("a", "bc", "d") {
		t.Fatal("length-prefixed scopes collided")
	}
}

func TestStateRecordsFirstError(t *testing.T) {
	ctx, state := WithState(context.Background())
	first := errors.New("first")
	RecordError(ctx, first)
	RecordError(ctx, errors.New("second"))
	if !errors.Is(state.Error(), first) {
		t.Fatalf("state error=%v, want first error", state.Error())
	}
}
