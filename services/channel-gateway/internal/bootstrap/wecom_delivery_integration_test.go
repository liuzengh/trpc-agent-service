package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	admissionapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	ad "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	connectionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	cd "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	eventadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	deliverypg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	wecomsender "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	routeapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/application"
)

type deliveryAdmissionResult struct {
	eventID string
	receipt ad.Receipt
}
type deliveryAdmissionCapture struct {
	source   *admissionapp.Service
	receipts chan deliveryAdmissionResult
}

func (c deliveryAdmissionCapture) AcceptInbound(ctx context.Context, in ad.Inbound) (ad.Receipt, error) {
	receipt, err := c.source.AcceptInbound(ctx, in)
	if err == nil {
		select {
		case c.receipts <- deliveryAdmissionResult{in.Key.EventID, receipt}:
		case <-ctx.Done():
			return receipt, ctx.Err()
		}
	}
	return receipt, err
}

// Decorate only the public ledger port to learn the committed Attempt identity.
// A1/A2/Observe/Finish remain the actual PostgreSQL implementation.
type deliveryObservationCapture struct {
	app.Ledger
	observations chan d.Observation
}

func (c deliveryObservationCapture) Observe(ctx context.Context, o d.Observation) error {
	if err := c.Ledger.Observe(ctx, o); err != nil {
		return err
	}
	select {
	case c.observations <- o:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
func deliveryReceive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("bounded WeCom Delivery fixture timed out")
		var zero T
		return zero
	}
}

func TestWeComDeliveryRealPGWSFinalAndOriginalOwnerFence(t *testing.T) { testWeComDelivery(t, false) }
func TestWeComRunnerRealPGWSFinalAndOriginalOwnerFence(t *testing.T)   { testWeComDelivery(t, true) }
func testWeComDelivery(t *testing.T, automatic bool) {
	pool := deliveryDB(t)
	routes := deliveryRoute(t, pool, "wecom")
	routeService, err := routeapp.NewService(routes)
	if err != nil {
		t.Fatal(err)
	}
	owners := connectionpg.NewStore(pool)
	admissions := admissionpg.NewStore(pool, routes).WithConnectionGuard(owners)
	inbound := admissionapp.New(admissions, routeBridge{routeService})
	received := make(chan deliveryAdmissionResult, 8)
	capture := deliveryAdmissionCapture{inbound, received}
	reader := admissionDeliveryReader{admissions}
	ledger, err := deliverypg.NewStore(pool, owners, deliverypg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	observations := make(chan d.Observation, 8)
	observedLedger := deliveryObservationCapture{ledger, observations}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const acceptedIntentID = "wecom-final-accepted"
	const finalText = "fixture final: 原消息的回复"
	peers := make(chan wecomPeer, 4)
	failures := make(chan string, 8)
	var finalCalls, authCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		peerCtx, stop := context.WithTimeout(r.Context(), 20*time.Second)
		defer stop()
		type frame struct {
			Cmd     string            `json:"cmd"`
			Headers map[string]string `json:"headers"`
			Body    json.RawMessage   `json:"body"`
		}
		var f frame
		_, raw, err := c.Read(peerCtx)
		if err != nil {
			return
		}
		var auth struct {
			BotID  string `json:"bot_id"`
			Secret string `json:"secret"`
		}
		if json.Unmarshal(raw, &f) != nil || f.Cmd != "aibot_subscribe" || json.Unmarshal(f.Body, &auth) != nil || auth.BotID != "fixture-bot" || auth.Secret != "delivery-fixture-secret" {
			failures <- "invalid subscribe"
			return
		}
		ack := func(headers map[string]string) error {
			raw, _ := json.Marshal(map[string]any{"headers": headers, "errcode": 0})
			return c.Write(peerCtx, websocket.MessageText, raw)
		}
		if ack(f.Headers) != nil {
			return
		}
		authCalls.Add(1)
		select {
		case peers <- wecomPeer{conn: c, ctx: peerCtx}:
		case <-ctx.Done():
			return
		}
		for {
			_, raw, err = c.Read(peerCtx)
			if err != nil {
				return
			}
			if json.Unmarshal(raw, &f) != nil {
				failures <- "invalid command frame"
				return
			}
			if f.Cmd == "ping" {
				if ack(f.Headers) != nil {
					return
				}
				continue
			}
			if f.Cmd != "aibot_respond_msg" {
				failures <- "unexpected command"
				return
			}
			finalCalls.Add(1)
			var body struct {
				MsgType string `json:"msgtype"`
				Stream  struct {
					ID, Content string
					Finish      bool
				} `json:"stream"`
			}
			if json.Unmarshal(f.Body, &body) != nil || body.MsgType != "stream" || body.Stream.ID == "" || body.Stream.Content != finalText || !body.Stream.Finish || f.Headers["req_id"] != "callback-first" {
				failures <- "Final changed original callback/content/finish"
				return
			}
			// Seeing the actual write must imply a committed A2, not merely CLAIMED.
			state, e := ledger.Get(peerCtx, acceptedIntentID)
			if e != nil || len(state.Parts) != 1 || state.Parts[0].State != d.Calling || state.Parts[0].AttemptNumber != 1 {
				failures <- "Final wrote before durable A2 CALLING"
				return
			}
			if ack(f.Headers) != nil {
				return
			}
		}
	}))
	defer server.Close()
	t.Setenv("DELIVERY_WECOM_FIXTURE_SECRET", "delivery-fixture-secret")
	account := cd.Account{ID: "account", BotID: "fixture-bot", CredentialRef: "DELIVERY_WECOM_FIXTURE_SECRET", Revision: 1, Enabled: true}
	factory := wecomClientFactory{acceptor: capture, url: "ws" + strings.TrimPrefix(server.URL, "http")}
	type running struct {
		supervisor *connection.Supervisor
		cancel     context.CancelFunc
		done       chan error
		closed     bool
	}
	var runs []*running
	defer func() {
		for _, r := range runs {
			r.cancel()
		}
		for _, r := range runs {
			if !r.closed {
				select {
				case err := <-r.done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(4 * time.Second):
					t.Error("Supervisor did not close")
				}
			}
		}
	}()
	start := func(id string) *running {
		t.Helper()
		s, err := connection.NewSupervisor(owners, accountFileSource{initial: []cd.Account{account}}, envWeComCredentials{}, factory, connection.Options{InstanceID: id, LeaseTTL: 1200 * time.Millisecond, PollInterval: 10 * time.Millisecond, OperationTimeout: 200 * time.Millisecond, DrainTimeout: 500 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(ctx)
		r := &running{supervisor: s, cancel: stop, done: make(chan error, 1)}
		runs = append(runs, r)
		go func() { r.done <- s.Run(runCtx) }()
		return r
	}
	firstOwner := start("owner-first")
	firstPeer := deliveryReceive(t, ctx, peers)
	eventually(t, func() bool { st, _ := firstOwner.supervisor.Status("account"); return st.Ready }, "WeCom first owner authenticated")
	firstPeer.send(t, "message-first", "callback-first", "original input", "aibot_msg_callback", "")
	first := deliveryReceive(t, ctx, received)
	if first.eventID != "message-first" || first.receipt.Decision != "admit-run" {
		t.Fatalf("first acceptance=%+v", first)
	}
	target, err := reader.ReadReplyTarget(ctx, first.receipt.AdmissionID, first.receipt.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if target.Origin == nil || target.Origin.InstanceID != "owner-first" || target.Origin.Epoch != 1 || target.Origin.Revision != 1 || target.Origin.SocketGeneration != 1 || target.CallbackRequestID != "callback-first" || target.ConversationID != "fixture-user" {
		t.Fatalf("original target=%+v", target)
	}
	intent := d.Intent{ID: acceptedIntentID, AdmissionID: first.receipt.AdmissionID, RunID: first.receipt.RunID, AttemptID: "execution-first", CompletionID: "completion-first", ExecutionGeneration: 1, Sequence: 1, Text: finalText, Deadline: time.Now().UTC().Add(time.Minute)}
	// This verifier represents an immutable committed Final fixture, not a Worker.
	proof := fixtureProof(t, intent, target)
	acceptor, err := app.NewAcceptor(ledger, reader, proof, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := eventadapter.NewHandler(acceptor)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := handler.Handle(ctx, finalWire(t, intent))
	if err != nil || receipt.PartCount != 1 {
		t.Fatalf("ReplyIntent accept=%+v %v", receipt, err)
	}
	provider, err := wecomsender.NewProvider(connectionDeliverySource{firstOwner.supervisor})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := app.NewDispatcher(observedLedger, provider, app.DispatchOptions{CallTimeout: time.Second, EvidenceTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	claim := d.ClaimRequest{Provider: "wecom", AccountID: "account", InstanceID: "owner-first", Owner: &d.OwnerFence{InstanceID: "owner-first", Epoch: target.Origin.Epoch, Revision: target.Origin.Revision}, Limit: 4, Lease: 5 * time.Second}
	count := 0
	if automatic {
		stopRunner := startDeliveryRunnerFixture(t, ctx, ledger, dispatcher, connectionDeliveryEligibility{source: firstOwner.supervisor, instanceID: "owner-first"}, "owner-first", "wecom")
		eventually(t, func() bool {
			state, e := ledger.Get(ctx, intent.ID)
			return e == nil && len(state.Parts) == 1 && state.Parts[0].State == d.Accepted
		}, "Runner delivered using current LocalOwner and original Sender")
		stopRunner()
	} else {
		count, err = dispatcher.DispatchAccount(ctx, claim)
		if err != nil || count != 1 {
			t.Fatalf("first dispatch=%d %v", count, err)
		}
	}
	state, err := ledger.Get(ctx, intent.ID)
	if err != nil || len(state.Parts) != 1 || state.Parts[0].State != d.Accepted || state.Parts[0].AttemptNumber != 1 {
		t.Fatalf("Final state=%+v %v", state, err)
	}
	observation := deliveryReceive(t, ctx, observations)
	stored, err := ledger.Observations(ctx, observation.AttemptID)
	if err != nil || len(stored) != 1 || stored[0].Result.Certainty != d.CertaintyAccepted || observation.ProviderRequestID != "callback-first" {
		t.Fatalf("durable observation=%+v %v", stored, err)
	}
	if count, err = dispatcher.DispatchAccount(ctx, claim); err != nil || count != 0 || finalCalls.Load() != 1 {
		t.Fatalf("accepted Final repeated: %d %v calls=%d", count, err, finalCalls.Load())
	}
	// A second accepted callback remains PENDING while ownership moves. Its immutable
	// reply origin must not be silently rebound to the replacement socket.
	firstPeer.send(t, "message-pending", "callback-pending", "pending input", "aibot_msg_callback", "")
	pending := deliveryReceive(t, ctx, received)
	pendingTarget, err := reader.ReadReplyTarget(ctx, pending.receipt.AdmissionID, pending.receipt.RunID)
	if err != nil {
		t.Fatal(err)
	}
	pendingIntent := d.Intent{ID: "wecom-final-stale", AdmissionID: pending.receipt.AdmissionID, RunID: pending.receipt.RunID, AttemptID: "execution-pending", CompletionID: "completion-pending", ExecutionGeneration: 1, Sequence: 1, Text: "must not migrate sockets", Deadline: time.Now().UTC().Add(time.Minute)}
	proof.proof = fixtureProof(t, pendingIntent, pendingTarget).proof
	if _, err = handler.Handle(ctx, finalWire(t, pendingIntent)); err != nil {
		t.Fatal(err)
	}
	firstOwner.cancel()
	if err = deliveryReceive(t, ctx, firstOwner.done); err != nil {
		t.Fatal(err)
	}
	firstOwner.closed = true
	secondOwner := start("owner-second")
	secondPeer := deliveryReceive(t, ctx, peers)
	eventually(t, func() bool { st, _ := secondOwner.supervisor.Status("account"); return st.Ready }, "WeCom second owner authenticated")
	secondStatus, _ := secondOwner.supervisor.Status("account")
	if secondStatus.Epoch <= pendingTarget.Origin.Epoch || authCalls.Load() != 2 {
		t.Fatalf("handover=%+v dials=%d", secondStatus, authCalls.Load())
	}
	secondPeer.send(t, "message-pending", "callback-replayed-new-socket", "pending input", "aibot_msg_callback", "")
	replay := deliveryReceive(t, ctx, received)
	if replay.receipt != pending.receipt {
		t.Fatal("provider replay created another Run")
	}
	replayTarget, err := reader.ReadReplyTarget(ctx, pending.receipt.AdmissionID, pending.receipt.RunID)
	if err != nil || replayTarget.Origin == nil || *replayTarget.Origin != *pendingTarget.Origin || replayTarget.CallbackRequestID != "callback-pending" {
		t.Fatal("replay overwrote original origin", err)
	}
	secondSource := connectionDeliverySource{secondOwner.supervisor}
	if _, err = secondSource.ReserveOriginal(ctx, pendingTarget); !errors.Is(err, d.ErrNotFound) {
		t.Fatalf("old origin reused by new owner: %v", err)
	}
	provider, err = wecomsender.NewProvider(secondSource)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err = app.NewDispatcher(observedLedger, provider, app.DispatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	claim.InstanceID = "owner-second"
	claim.Owner = &d.OwnerFence{InstanceID: "owner-second", Epoch: secondStatus.Epoch, Revision: secondStatus.Revision}
	if count, err = dispatcher.DispatchAccount(ctx, claim); err != nil || count != 1 {
		t.Fatalf("stale preparation=%d %v", count, err)
	}
	stale, err := ledger.Get(ctx, pendingIntent.ID)
	if err != nil || len(stale.Parts) != 1 || stale.Parts[0].State != d.NotSent || stale.Parts[0].AttemptNumber != 0 || finalCalls.Load() != 1 {
		t.Fatalf("stale origin sent/A2 advanced: %+v %v calls=%d", stale, err, finalCalls.Load())
	}
	if count, err = dispatcher.DispatchAccount(ctx, claim); err != nil || count != 0 {
		t.Fatalf("stale origin retried: %d %v", count, err)
	}
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
	secondOwner.cancel()
	if err = deliveryReceive(t, ctx, secondOwner.done); err != nil {
		t.Fatal(err)
	}
	secondOwner.closed = true
	t.Log("WECOM_DELIVERY_VERIFIED: real PG Routing/Admission/ReplyOrigin; original callback; Supervisor/owner lease; SDK actual WS Final; committed A2 before Write; exact ACK; durable Observation+Finish; no repeated ACCEPTED; owner handover; receipt replay retains original origin; stale origin NOT_SENT before A2; Execution committedFinalFixture only, no real Worker/NATS/WeCom account")
}
