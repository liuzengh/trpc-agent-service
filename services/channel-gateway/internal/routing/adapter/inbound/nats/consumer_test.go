package natsadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeInspector struct {
	info *jetstream.StreamInfo
	err  error
}

func (s fakeInspector) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return s.info, s.err
}

type fakeMessage struct {
	jetstream.Msg
	data        []byte
	metadata    *jetstream.MsgMetadata
	metadataErr error
	subject     string
	acks        int
	onAck       func()
}

func (m *fakeMessage) Data() []byte                              { return m.data }
func (m *fakeMessage) Metadata() (*jetstream.MsgMetadata, error) { return m.metadata, m.metadataErr }
func (m *fakeMessage) Subject() string                           { return m.subject }
func (m *fakeMessage) DoubleAck(context.Context) error {
	m.acks++
	if m.onAck != nil {
		m.onAck()
	}
	return nil
}

type fakeApplier struct {
	position                                              domain.StreamPosition
	event                                                 domain.RouteEvent
	beginSource                                           domain.ReplaySource
	beginCalls, applyCalls, observeCalls, quarantineCalls int
	reason                                                domain.QuarantineReason
	applyErr, errorOnQuarantine                           error
	onBegin                                               func()
}

func (a *fakeApplier) BeginReplay(_ context.Context, s domain.ReplaySource) error {
	a.beginCalls++
	a.beginSource = s
	if a.onBegin != nil {
		a.onBegin()
	}
	return nil
}
func (a *fakeApplier) ObserveSource(context.Context, domain.ReplaySource) error {
	a.observeCalls++
	return nil
}
func (a *fakeApplier) ApplyFromStream(_ context.Context, p domain.StreamPosition, e domain.RouteEvent) error {
	a.position = p
	a.event = e
	a.applyCalls++
	return a.applyErr
}
func (a *fakeApplier) Quarantine(_ context.Context, p domain.StreamPosition, r domain.QuarantineReason, digest string) error {
	a.quarantineCalls++
	a.reason = r
	a.position = p
	return a.errorOnQuarantine
}
func consumerFixture() (*Consumer, *fakeApplier, *fakeMessage, domain.ReplaySource) {
	info := &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "CONTROL_ROUTES", Subjects: []string{RouteSubject}}, Created: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), State: jetstream.StreamState{FirstSeq: 1, LastSeq: 7, Msgs: 7}}
	event := domain.RouteEvent{EventID: "evt-7", SchemaVersion: 1, Enabled: true, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", TenantID: "tenant-1", BindingID: "binding-1", Generation: 2, DeploymentRevisionID: "revision-1", ManifestRef: "manifest/revision-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}}
	data, _ := json.Marshal(event)
	msg := &fakeMessage{data: data, subject: RouteSubject, metadata: &jetstream.MsgMetadata{Stream: "CONTROL_ROUTES", Sequence: jetstream.SequencePair{Stream: 7, Consumer: 2}}}
	a := &fakeApplier{}
	return New(nil, fakeInspector{info: info}, a), a, msg, sourceFromInfo(info)
}
func TestConsumerUsesTrustedStreamPositionAndACKsAfterApply(t *testing.T) {
	c, a, msg, source := consumerFixture()
	msg.onAck = func() {
		if a.applyCalls != 1 || a.applyErr != nil {
			t.Fatal("ACK before durable Apply result")
		}
	}
	if err := c.process(context.Background(), msg, source, true); err != nil {
		t.Fatal(err)
	}
	if a.position.Sequence != 7 || a.position.StreamID != source.StreamID || a.position.StreamName != source.StreamName || a.event.Route.Generation != 2 {
		t.Fatalf("untrusted position mapping: %#v %#v", a.position, a.event)
	}
	if msg.acks != 1 || a.observeCalls != 1 {
		t.Fatalf("acks=%d observations=%d", msg.acks, a.observeCalls)
	}
}

func TestConsumerMapsCompleteTrafficPolicy(t *testing.T) {
	c, a, msg, source := consumerFixture()
	event := domain.RouteEvent{
		EventID:       "evt-traffic",
		SchemaVersion: 1,
		Enabled:       true,
		Route: domain.RouteSnapshot{
			Provider:             "telegram",
			AccountID:            "account-1",
			TenantID:             "tenant-1",
			BindingID:            "binding-1",
			Generation:           3,
			DeploymentRevisionID: "revision-stable",
			ManifestRef:          "manifest/revision-stable",
			ManifestDigest:       "sha256:" + strings.Repeat("a", 64),
			Traffic: &domain.TrafficRollout{
				RolloutID: "rollout-1",
				Target: domain.PublishedTarget{
					TenantID:             "tenant-1",
					DeploymentID:         "deployment-canary",
					RevisionNumber:       2,
					DeploymentRevisionID: "revision-canary",
					ManifestRef:          "manifest/revision-canary",
					ManifestDigest:       "sha256:" + strings.Repeat("b", 64),
				},
				PercentageBasisPoints: 1200,
				CanarySubjects:        []string{},
			},
		},
	}
	msg.data, _ = json.Marshal(event)
	if err := c.process(context.Background(), msg, source, true); err != nil {
		t.Fatal(err)
	}
	if a.event.Route.Traffic == nil || a.event.Route.Traffic.Target.DeploymentRevisionID != "revision-canary" || a.event.Route.Traffic.PercentageBasisPoints != 1200 || a.event.Route.Traffic.CanarySubjects == nil || len(a.event.Route.Traffic.CanarySubjects) != 0 {
		t.Fatalf("traffic policy was not mapped: %+v", a.event.Route.Traffic)
	}
}
func TestConsumerTransientDatabaseOrStreamFailureDoesNotACK(t *testing.T) {
	for _, kind := range []string{"database", "stream"} {
		t.Run(kind, func(t *testing.T) {
			c, a, msg, source := consumerFixture()
			transient := errors.New("temporary failure")
			if kind == "database" {
				a.applyErr = transient
			} else {
				c.stream = fakeInspector{err: transient}
			}
			if err := c.process(context.Background(), msg, source, false); !errors.Is(err, transient) {
				t.Fatal(err)
			}
			if msg.acks != 0 || a.quarantineCalls != 0 {
				t.Fatal("transient failure ACKed or quarantined")
			}
		})
	}
}
func TestConsumerQuarantinesPermanentFaultBeforeACK(t *testing.T) {
	for _, kind := range []string{"schema", "source-recreated", "history-gap", "metadata", "subject", "domain-conflict"} {
		t.Run(kind, func(t *testing.T) {
			c, a, msg, source := consumerFixture()
			switch kind {
			case "schema":
				msg.data = []byte(`{"schema_version":2}`)
			case "source-recreated":
				info := *c.stream.(fakeInspector).info
				info.Created = info.Created.Add(time.Second)
				c.stream = fakeInspector{info: &info}
			case "history-gap":
				info := *c.stream.(fakeInspector).info
				info.State.Msgs = 6
				c.stream = fakeInspector{info: &info}
			case "metadata":
				msg.metadataErr = errors.New("invalid ack subject")
			case "subject":
				msg.subject = "other.subject"
			case "domain-conflict":
				a.applyErr = domain.ErrProjectionBlocked
			}
			msg.onAck = func() {
				if kind != "domain-conflict" && a.quarantineCalls != 1 {
					t.Fatal("ACK before durable quarantine")
				}
			}
			if err := c.process(context.Background(), msg, source, false); !errors.Is(err, domain.ErrProjectionBlocked) {
				t.Fatal(err)
			}
			if msg.acks != 1 {
				t.Fatalf("acks=%d", msg.acks)
			}
		})
	}
}
func TestFailedQuarantineRemainsUnacknowledged(t *testing.T) {
	c, a, msg, source := consumerFixture()
	msg.data = []byte("{}")
	a.errorOnQuarantine = errors.New("postgres unavailable")
	if err := c.process(context.Background(), msg, source, false); !errors.Is(err, a.errorOnQuarantine) {
		t.Fatal(err)
	}
	if msg.acks != 0 {
		t.Fatal("quarantine failed but event ACKed")
	}
}
func TestIdleSourceObservationRefreshesWithoutBusinessEvent(t *testing.T) {
	c, a, _, source := consumerFixture()
	if err := c.observe(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if a.observeCalls != 1 || a.applyCalls != 0 {
		t.Fatalf("observe=%d apply=%d", a.observeCalls, a.applyCalls)
	}
}

type neverConsumer struct{}

func (neverConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) {
	panic("Next after cancelled startup")
}
func TestRunCapturesStartupWatermarkIncludingEmptyStream(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "populated", true: "empty"}[empty], func(t *testing.T) {
			c, a, _, _ := consumerFixture()
			info := *c.stream.(fakeInspector).info
			if empty {
				info.State = jetstream.StreamState{}
			}
			c.stream = fakeInspector{info: &info}
			c.consumer = neverConsumer{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.onBegin = cancel
			if err := c.Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if a.beginCalls != 1 || a.beginSource.LastSequence != info.State.LastSeq || a.beginSource.StreamID != info.Created.UTC().Format(time.RFC3339Nano) {
				t.Fatalf("captured=%#v", a.beginSource)
			}
		})
	}
}

func TestInitializeCapturesOfflineBacklogBeforeRun(t *testing.T) {
	c, a, _, _ := consumerFixture()
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	info := *c.stream.(fakeInspector).info
	info.State.LastSeq = 8
	info.State.Msgs = 8
	c.stream = fakeInspector{info: &info}
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.beginSource.LastSequence != 8 || a.beginCalls != 2 || a.applyCalls != 0 {
		t.Fatalf("startup cut=%#v begin=%d apply=%d", a.beginSource, a.beginCalls, a.applyCalls)
	}
	source, initialized := c.initializedSource()
	if !initialized || source.LastSequence != 8 {
		t.Fatalf("source=%#v initialized=%v", source, initialized)
	}
}
