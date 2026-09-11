package postgresadapter_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

func TestReplyOriginFirstCommitReplayAndLegacyNull(t *testing.T) {
	store, pool, _ := setupWeCom(t)
	store = store.WithConnectionGuard(ownerGuardFunc(func(context.Context, pgx.Tx, string, string, int64, int64) error { return nil }))
	first := wecomAcceptance("origin-first")
	first.Input.ReplyOrigin = &domain.ReplyOrigin{InstanceID: "owner-a", Epoch: 7, Revision: 3, SocketGeneration: 13}
	ctx := context.Background()
	receipt, err := store.Commit(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	replay := first
	replay.Input.ConnectionFence = &domain.ConnectionFence{InstanceID: "owner-b", Epoch: 8, Revision: 3}
	replay.Input.ReplyOrigin = &domain.ReplyOrigin{InstanceID: "owner-b", Epoch: 8, Revision: 3, SocketGeneration: 1}
	replay.Input.ReplyContext = json.RawMessage(`{"chat_type":"single","chatid_or_userid":"100","callback_req_id":"new-socket-req","received_at":"2026-09-05T00:00:00Z"}`)
	if got, err := store.Commit(ctx, replay); err != nil || got != receipt {
		t.Fatalf("replay=%+v %v", got, err)
	}
	var raw, input, payload []byte
	if err = pool.QueryRow(ctx, `SELECT reply_origin,input FROM gateway_admissions WHERE admission_id=$1`, receipt.AdmissionID).Scan(&raw, &input); err != nil {
		t.Fatal(err)
	}
	var saved domain.ReplyOrigin
	if err = json.Unmarshal(raw, &saved); err != nil || saved != *first.Input.ReplyOrigin {
		t.Fatalf("stored origin=%+v %v", saved, err)
	}
	if err = pool.QueryRow(ctx, `SELECT payload FROM gateway_outbox WHERE event_id=$1`, receipt.AdmissionID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{string(input), string(payload)} {
		for _, forbidden := range []string{"reply_origin", "socket_generation", "owner-a", "owner-b", "new-socket-req"} {
			if strings.Contains(content, forbidden) {
				t.Fatalf("origin/replay polluted wire: %s", forbidden)
			}
		}
	}
	old := wecomAcceptance("origin-old")
	old.Input.ReplyOrigin = nil
	if _, err = store.Commit(ctx, old); err != nil {
		t.Fatal(err)
	}
	var isNull bool
	if err = pool.QueryRow(ctx, `SELECT reply_origin IS NULL FROM gateway_admissions WHERE admission_id=$1`, old.Receipt.AdmissionID).Scan(&isNull); err != nil || !isNull {
		t.Fatalf("legacy null=%v %v", isNull, err)
	}
}
