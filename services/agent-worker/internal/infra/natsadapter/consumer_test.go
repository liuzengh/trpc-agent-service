package natsadapter

import (
	"context"
	"errors"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"os"
	"strings"
	"testing"
	"time"

	execution "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type testMessage struct {
	headers nats.Header
	jetstream.Msg
	raw        []byte
	subject    string
	meta       *jetstream.MsgMetadata
	acks, naks int
	ackErr     error
	onAck      func()
}

func (m *testMessage) Headers() nats.Header { return m.headers }

func (m *testMessage) Data() []byte                              { return m.raw }
func (m *testMessage) Subject() string                           { return m.subject }
func (m *testMessage) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *testMessage) DoubleAck(context.Context) error {
	m.acks++
	if m.onAck != nil {
		m.onAck()
	}
	return m.ackErr
}
func (m *testMessage) NakWithDelay(time.Duration) error { m.naks++; return nil }

type testStream struct {
	info *jetstream.StreamInfo
	err  error
}

func (s testStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return s.info, s.err
}

type testConsumer struct {
	msg jetstream.Msg
	err error
}

func (c testConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) { return c.msg, c.err }

type intake struct {
	ctx   context.Context
	calls int
	err   error
	last  execution.Requested
}

func (i *intake) Accept(ctx context.Context, r execution.Requested) (execution.Receipt, error) {
	i.ctx = ctx
	i.calls++
	i.last = r
	return execution.Receipt{EventID: r.EventID, RunID: r.RunID, TenantID: r.Route.TenantID}, i.err
}

type projection struct {
	calls int
	err   error
	last  manifest.Publication
}

func (p *projection) Apply(_ context.Context, m manifest.Publication, capacity int) error {
	p.calls++
	p.last = m
	return p.err
}

type rejector struct {
	calls                  int
	err                    error
	source, digest, reason string
}

func (r *rejector) Reject(_ context.Context, source, digest, reason string) error {
	r.calls++
	r.source = source
	r.digest = digest
	r.reason = reason
	return r.err
}
func fixture(t *testing.T, isManifest bool) (*Consumer, *testMessage, *intake, *projection, *rejector) {
	t.Helper()
	name, subject, durable, path := RunStream, RunSubject, RunDurable, "../../../../../api/events/execution/v1/fixtures/valid/telegram-text.json"
	if isManifest {
		name, subject, durable, path = ManifestStream, ManifestSubject, ManifestDurable, "../../../../../api/events/control/v1/examples/valid/runtime-manifest-published.json"
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	m := &testMessage{raw: raw, subject: subject, meta: &jetstream.MsgMetadata{Stream: name, Consumer: durable, Timestamp: now, Sequence: jetstream.SequencePair{Stream: 3}}}
	stream := testStream{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: name, Subjects: []string{subject}}, Created: now.Add(-time.Minute), State: jetstream.StreamState{LastSeq: 3}}}
	i, p, r := &intake{}, &projection{}, &rejector{}
	var c *Consumer
	if isManifest {
		c, e = NewManifest(testConsumer{msg: m}, stream, p, r, 10)
	} else {
		c, e = NewRun(testConsumer{msg: m}, stream, i, r)
	}
	if e != nil {
		t.Fatal(e)
	}
	return c, m, i, p, r
}
func TestDurableRunAndManifestBeforeACK(t *testing.T) {
	for _, isManifest := range []bool{false, true} {
		c, m, i, p, r := fixture(t, isManifest)
		m.onAck = func() {
			if i.calls+p.calls != 1 || r.calls != 0 {
				t.Fatal("ACK before owner transaction")
			}
		}
		found, e := c.Poll(context.Background())
		if e != nil || !found || m.acks != 1 || m.naks != 0 {
			t.Fatal(e, found, m.acks, m.naks)
		}
	}
}
func TestPersistedOwnerConflictACKsWithoutNewRejection(t *testing.T) {
	for _, isManifest := range []bool{false, true} {
		c, m, i, p, r := fixture(t, isManifest)
		i.err = execution.ErrConflict
		p.err = manifest.ErrConflict
		if e := c.Handle(context.Background(), m); e != nil || m.acks != 1 || r.calls != 0 {
			t.Fatal(e, m.acks, r.calls)
		}
	}
}
func TestMalformedWireRejectionCommitBeforeACK(t *testing.T) {
	for _, isManifest := range []bool{false, true} {
		c, m, i, p, r := fixture(t, isManifest)
		m.raw = []byte(`{"event_id":"duplicate","event_id":"bad"}`)
		m.onAck = func() {
			if r.calls != 1 {
				t.Fatal("ACK before rejection")
			}
		}
		if e := c.Handle(context.Background(), m); e != nil || m.acks != 1 || i.calls+p.calls != 0 {
			t.Fatal(e, m.acks)
		}
		if !strings.Contains(r.source, "#3") || !strings.Contains(r.source, "@") || r.digest != execution.Digest(m.raw) {
			t.Fatal("untrusted reject identity", r)
		}
	}
}
func TestFailedRejectionAndTransientOwnersDelayNAK(t *testing.T) {
	for _, kind := range []string{"Run PG", "Manifest capacity", "reject PG", "source"} {
		c, m, i, p, r := fixture(t, kind == "Manifest capacity")
		switch kind {
		case "Run PG":
			i.err = errors.New("database unavailable")
		case "Manifest capacity":
			p.err = manifest.ErrCapacity
		case "reject PG":
			m.raw = []byte(`{}`)
			r.err = errors.New("database unavailable")
		case "source":
			c.stream = testStream{err: ErrUnavailable}
		}
		_, err := c.Poll(context.Background())
		if err == nil || m.acks != 0 || m.naks != 1 {
			t.Fatal(kind, err, m.acks, m.naks)
		}
	}
}
func TestOldBrokerIncarnationAndWrongDurableDoNotACK(t *testing.T) {
	for _, kind := range []string{"created", "durable", "sequence"} {
		c, m, _, _, r := fixture(t, false)
		switch kind {
		case "created":
			s := *c.stream.(testStream).info
			s.Created = m.meta.Timestamp.Add(time.Second)
			c.stream = testStream{info: &s}
		case "durable":
			m.meta.Consumer = "wrong"
		case "sequence":
			m.meta.Sequence.Stream = 4
		}
		if e := c.Handle(context.Background(), m); !errors.Is(e, ErrTopology) || m.acks != 0 || r.calls != 0 {
			t.Fatal(kind, e, m.acks, r.calls)
		}
	}
}

type outbox struct {
	rows             []execution.OutboxItem
	marks            int
	readErr, markErr error
}

func (o *outbox) PendingReplies(context.Context, int) ([]execution.OutboxItem, error) {
	return o.rows, o.readErr
}
func (o *outbox) MarkReplyPublished(context.Context, string, string) error {
	o.marks++
	return o.markErr
}

type publisher struct {
	calls     int
	payloads  [][]byte
	ack       *jetstream.PubAck
	err       error
	onPublish func()
}

func (p *publisher) PublishMsg(_ context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.calls++
	p.payloads = append(p.payloads, append([]byte(nil), msg.Data...))
	if p.onPublish != nil {
		p.onPublish()
	}
	return p.ack, p.err
}

func TestSourceValidationRejectsInPlaceStreamAndDurableDrift(t *testing.T) {
	for _, isManifest := range []bool{false, true} {
		name, subject, durable, retention := RunStream, RunSubject, RunDurable, jetstream.WorkQueuePolicy
		validate := ValidateRunSource
		if isManifest {
			name, subject, durable, retention = ManifestStream, ManifestSubject, ManifestDurable, jetstream.LimitsPolicy
			validate = ValidateManifestSource
		}
		stream := &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Retention: retention, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew, MaxBytes: 1 << 20, MaxMsgSize: 1 << 20, Replicas: 1, Duplicates: 2 * time.Minute, DenyDelete: true, DenyPurge: true}}
		consumer := &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Durable: durable, AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy, FilterSubject: subject, MaxDeliver: -1, MaxAckPending: 64, AckWait: 30 * time.Second, ReplayPolicy: jetstream.ReplayInstantPolicy}}
		if err := validate(stream, consumer); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"maxage", "eviction", "retention", "filter", "delivery", "pause", "headers"} {
			s, c := *stream, *consumer
			switch kind {
			case "maxage":
				s.Config.MaxAge = time.Minute
			case "eviction":
				s.Config.Discard = jetstream.DiscardOld
			case "retention":
				s.Config.Retention = jetstream.InterestPolicy
			case "filter":
				c.Config.FilterSubject = "wrong"
			case "delivery":
				c.Config.DeliverPolicy = jetstream.DeliverNewPolicy
			case "pause":
				future := time.Now().Add(time.Hour)
				c.Config.PauseUntil = &future
			case "headers":
				c.Config.HeadersOnly = true
			}
			if validate(&s, &c) == nil {
				t.Fatal("in-place drift accepted", isManifest, kind)
			}
		}
	}
}
func (o *outbox) PendingTracedReplies(ctx context.Context, limit int) ([]app.TracedReply, error) {
	rows, err := o.PendingReplies(ctx, limit)
	out := make([]app.TracedReply, 0, len(rows))
	for _, row := range rows {
		out = append(out, app.TracedReply{OutboxItem: row})
	}
	return out, err

}
