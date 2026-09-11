package natsadapter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	event "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type inspector struct {
	info *jetstream.StreamInfo
	err  error
}

func (s inspector) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return s.info, s.err
}

type message struct {
	jetstream.Msg
	headers nats.Header
	data    []byte
	meta    *jetstream.MsgMetadata
	acks    int
	onAck   func()
	ackErr  error
}

func (m *message) Data() []byte                              { return m.data }
func (m *message) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *message) Subject() string                           { return "execution.reply-intent.v1" }
func (m *message) DoubleAck(context.Context) error {
	m.acks++
	if m.onAck != nil {
		m.onAck()
	}
	return m.ackErr
}

type receipts struct {
	rows              map[d.TransportPosition]d.TransportReceipt
	readErr, writeErr error
	writes            int
}

func (s *receipts) FindTransportReceipt(_ context.Context, p d.TransportPosition) (d.TransportReceipt, bool, error) {
	r, ok := s.rows[p]
	return r, ok, s.readErr
}
func (s *receipts) RecordTransportReceipt(_ context.Context, r d.TransportReceipt) error {
	s.writes++
	if s.writeErr != nil {
		return s.writeErr
	}
	s.rows[r.Position] = r
	return nil
}

type handler struct {
	err   error
	calls int
}

func (h *handler) Handle(context.Context, []byte) (d.Receipt, error) {
	h.calls++
	return d.Receipt{IntentID: "intent", RunID: "run", PartCount: 1}, h.err
}
func fixture() (*Consumer, *handler, *receipts, *message) {
	now := time.Now().UTC()
	h := &handler{}
	s := &receipts{rows: map[d.TransportPosition]d.TransportReceipt{}}
	m := &message{data: []byte(`{"opaque":"bytes"}`), meta: &jetstream.MsgMetadata{Stream: StreamName, Consumer: DurableName, Sequence: jetstream.SequencePair{Stream: 1}, Timestamp: now}}
	c := &Consumer{handler: h, receipts: s, stream: inspector{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: StreamName, Subjects: []string{"execution.reply-intent.v1"}}, Created: now.Add(-time.Minute), State: jetstream.StreamState{LastSeq: 1}}}}
	return c, h, s, m
}
func TestReplyACKOnlyAfterTransportCommitAndReplayFirst(t *testing.T) {
	c, h, s, m := fixture()
	m.onAck = func() {
		if len(s.rows) != 1 {
			t.Fatal("ACK before receipt")
		}
	}
	if err := c.process(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	h.err = d.ErrUnavailable
	if err := c.process(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if h.calls != 1 || m.acks != 2 || s.writes != 1 {
		t.Fatal("replay contacted dependencies", h.calls, m.acks, s.writes)
	}
}
func TestPermanentReplyResultsPersistBeforeACK(t *testing.T) {
	for _, err := range []error{d.ErrInvalid, d.ErrConflict, d.ErrUnauthorized, d.ErrExpired, d.ErrUnsupported} {
		t.Run(err.Error(), func(t *testing.T) {
			c, h, s, m := fixture()
			h.err = err
			m.onAck = func() {
				if len(s.rows) != 1 {
					t.Fatal("ACK before rejection")
				}
				for _, r := range s.rows {
					if r.Outcome != "REJECTED" || r.Reason != permanentReason(err) {
						t.Fatal(r)
					}
				}
			}
			if e := c.process(context.Background(), m); e != nil || m.acks != 1 {
				t.Fatal(e, m.acks)
			}
		})
	}
}
func TestTransientReplyResultsNeverACK(t *testing.T) {
	for _, kind := range []string{"postgres-read", "postgres-write", "proof", "capacity", "absent", "stream"} {
		t.Run(kind, func(t *testing.T) {
			c, h, s, m := fixture()
			switch kind {
			case "postgres-read":
				s.readErr = d.ErrUnavailable
			case "postgres-write":
				s.writeErr = d.ErrUnavailable
			case "proof":
				h.err = d.ErrUnavailable
			case "capacity":
				h.err = d.ErrCapacity
			case "absent":
				h.err = d.ErrNotFound
			case "stream":
				c.stream = inspector{err: d.ErrUnavailable}
			}
			if err := c.process(context.Background(), m); err == nil || m.acks != 0 {
				t.Fatal("transient ACKed", err, m.acks)
			}
		})
	}
}
func TestRejectedReceiptWriteFailureNeverACK(t *testing.T) {
	c, h, s, m := fixture()
	h.err = d.ErrUnsupported
	s.writeErr = d.ErrUnavailable
	if err := c.process(context.Background(), m); err == nil || m.acks != 0 {
		t.Fatal(err, m.acks)
	}
}
func TestSourceRecreationDoesNotReuseOldMessageIdentity(t *testing.T) {
	c, _, s, m := fixture()
	copy := *c.stream.(inspector).info
	copy.Created = m.meta.Timestamp.Add(time.Second)
	c.stream = inspector{info: &copy}
	if err := c.process(context.Background(), m); err == nil || m.acks != 0 || s.writes != 0 {
		t.Fatal("old incarnation accepted", err)
	}
}
func TestUncertainACKReplaysTransportReceipt(t *testing.T) {
	c, h, s, m := fixture()
	m.ackErr = errors.New("ack disconnected")
	if c.process(context.Background(), m) == nil {
		t.Fatal("ack uncertainty lost")
	}
	m.ackErr = nil
	h.err = d.ErrUnavailable
	if err := c.process(context.Background(), m); err != nil || h.calls != 1 || s.writes != 1 {
		t.Fatal(err, h.calls, s.writes)
	}
}

type businessLedger struct {
	app.Ledger
	r       d.Receipt
	digest  string
	commits int
}

func (l *businessLedger) Find(context.Context, string) (d.Receipt, string, bool, error) {
	return l.r, l.digest, l.commits > 0, nil
}
func (l *businessLedger) Accept(_ context.Context, p d.Prepared) (d.Receipt, error) {
	l.commits++
	l.digest = p.Digest
	l.r = d.Receipt{IntentID: p.Intent.ID, RunID: p.Intent.RunID, PartCount: len(p.Parts)}
	return l.r, nil
}

type admission struct{ calls int }

func (a *admission) ReadReplyTarget(context.Context, string, string) (d.Target, error) {
	a.calls++
	return d.Target{TenantID: "tenant", Provider: "telegram", AccountID: "account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "123", SourceEventID: "source", ReceivedAt: time.Now().UTC()}, nil
}

type proof struct {
	calls int
	err   error
}

func (p *proof) VerifyCommittedFinal(_ context.Context, i d.Intent, digest string) (app.FinalAuthorization, error) {
	p.calls++
	return app.FinalAuthorization{IntentID: i.ID, Digest: digest, AdmissionID: i.AdmissionID, RunID: i.RunID, AttemptID: i.AttemptID, CompletionID: i.CompletionID, ExecutionGeneration: i.ExecutionGeneration, Sequence: i.Sequence, TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}, p.err
}
func TestDeliveryCommitBeforeTransportCrashReplaysBusinessReceipt(t *testing.T) {
	c, _, s, m := fixture()
	l := &businessLedger{}
	a := &admission{}
	p := &proof{}
	acceptor, err := app.NewAcceptor(l, a, p, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := event.NewHandler(acceptor)
	if err != nil {
		t.Fatal(err)
	}
	c.handler = h
	m.data = []byte(`{"schema_version":1,"intent_id":"intent","admission_id":"admission","run_id":"run","kind":"final","sequence":1,"execution":{"attempt_id":"attempt","completion_id":"completion","generation":1},"content":{"type":"text","text":"Final"},"deadline":"2030-01-01T00:00:00Z"}`)
	s.writeErr = d.ErrUnavailable
	if err = c.process(context.Background(), m); err == nil || m.acks != 0 || l.commits != 1 {
		t.Fatal(err, m.acks, l.commits)
	}
	// The first SQL transaction committed; the second did not. Restart/replay
	// must bypass now-offline proof and stopped Acceptor through business receipt.
	s.writeErr = nil
	p.err = d.ErrUnavailable
	acceptor.Stop()
	if err = c.process(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if l.commits != 1 || p.calls != 1 || a.calls != 1 || m.acks != 1 {
		t.Fatal(l.commits, p.calls, a.calls, m.acks)
	}
}
func TestStrictWireRejectsBeforeDelivery(t *testing.T) {
	c, _, s, m := fixture()
	l := &businessLedger{}
	a := &admission{}
	p := &proof{}
	acceptor, _ := app.NewAcceptor(l, a, p, app.AcceptOptions{})
	h, _ := event.NewHandler(acceptor)
	c.handler = h
	m.data = []byte(`{"schema_version":1,"schema_version":1}`)
	if err := c.process(context.Background(), m); err != nil || m.acks != 1 || l.commits != 0 || a.calls != 0 {
		t.Fatal(err, m.acks, l.commits, a.calls)
	}
	for _, r := range s.rows {
		if r.Reason != "INVALID_WIRE" {
			t.Fatal(r)
		}
	}
}

type onceConsumer struct{ message jetstream.Msg }

func (c *onceConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) { return c.message, nil }

type nakMessage struct {
	*message
	naks   int
	delay  time.Duration
	cancel context.CancelFunc
}

func (m *nakMessage) NakWithDelay(delay time.Duration) error {
	m.naks++
	m.delay = delay
	m.cancel()
	return nil
}
func TestRunDelaysNAKWithoutACKOnTransientProof(t *testing.T) {
	c, h, _, m := fixture()
	h.err = d.ErrUnavailable
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := &nakMessage{message: m, cancel: cancel}
	c.consumer = &onceConsumer{message: msg}
	if err := c.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if msg.naks != 1 || msg.delay != time.Second || m.acks != 0 {
		t.Fatal(msg.naks, msg.delay, m.acks)
	}
}

func (m *message) Headers() nats.Header { return m.headers }
