package trpcagent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type summaryFixtureModel struct {
	calls atomic.Int32
	fail  bool
}

func (*summaryFixtureModel) Info() model.Info { return model.Info{Name: "summary-fixture"} }
func (m *summaryFixtureModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.calls.Add(1)
	if m.fail {
		return nil, errors.New("fixture summary model failed")
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("User prefers green tea.")}}}
	close(ch)
	return ch, nil
}
func summaryManifestFixture() protocol.ManifestContent {
	c := manifestCapabilityFixture()
	c.Runtime = &protocol.ManifestRuntime{Summary: &protocol.ManifestSummary{Enabled: true, ModelResource: "summarizer", EventThreshold: 2}}
	c.Resources.Models["summarizer"] = protocol.ManifestModelResource{Kind: "openai_compatible", AdapterVersion: "openai-compatible-v1", Model: "summary-fixture", Capabilities: []string{"chat"}}
	return c
}
func summaryFixtureEvent(id, text string) *event.Event {
	return &event.Event{ID: id, Author: "user", Timestamp: time.Now().UTC(), Response: &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewUserMessage(text)}}}}
}
func TestManifestSummarizerUsesExplicitModelAndThreshold(t *testing.T) {
	c := summaryManifestFixture()
	m := &summaryFixtureModel{}
	s, err := BuildManifestSummarizer(c, map[string]model.Model{"summarizer": m})
	if err != nil {
		t.Fatal(err)
	}
	sess := session.NewSession("app", "user", "session")
	sess.UpdateUserSession(summaryFixtureEvent("one", "I like green tea"))
	if s.ShouldSummarize(sess) {
		t.Fatal("ignored explicit event threshold")
	}
	sess.UpdateUserSession(summaryFixtureEvent("two", "Please remember that"))
	if s.ShouldSummarize(sess) {
		t.Fatal("SDK threshold is strictly exceeded, not inclusive")
	}
	sess.UpdateUserSession(summaryFixtureEvent("three", "Thank you"))
	if !s.ShouldSummarize(sess) {
		t.Fatal("threshold not exceeded")
	}
	text, err := s.Summarize(context.Background(), sess)
	if err != nil || text != "User prefers green tea." || m.calls.Load() != 1 {
		t.Fatalf("summary=%q calls=%d err=%v", text, m.calls.Load(), err)
	}
	c.Runtime = nil
	if s, err = BuildManifestSummarizer(c, map[string]model.Model{"summarizer": m}); err != nil || s != nil || m.calls.Load() != 1 {
		t.Fatal("implicitly enabled summary")
	}
}
func TestManifestSummarizerRejectsMissingBindings(t *testing.T) {
	for _, mode := range []string{"runtime-empty", "disabled", "threshold", "missing-resource", "no-chat", "missing-model", "typed-nil", "wrong-model", "missing-session", "wrong-session-role"} {
		t.Run(mode, func(t *testing.T) {
			c := summaryManifestFixture()
			models := map[string]model.Model{"summarizer": &summaryFixtureModel{}}
			switch mode {
			case "runtime-empty":
				c.Runtime.Summary = nil
			case "disabled":
				c.Runtime.Summary.Enabled = false
			case "threshold":
				c.Runtime.Summary.EventThreshold = 0
			case "missing-resource":
				delete(c.Resources.Models, "summarizer")
			case "no-chat":
				r := c.Resources.Models["summarizer"]
				r.Capabilities = nil
				c.Resources.Models["summarizer"] = r
			case "missing-model":
				delete(models, "summarizer")
			case "typed-nil":
				models["summarizer"] = (*summaryFixtureModel)(nil)
			case "wrong-model":
				r := c.Resources.Models["summarizer"]
				r.Model = "different"
				c.Resources.Models["summarizer"] = r
			case "missing-session":
				delete(c.StorageRoles, "session")
			case "wrong-session-role":
				c.StorageRoles["session"] = "memory"
			}
			s, err := BuildManifestSummarizer(c, models)
			if !errors.Is(err, ErrManifestCapabilities) || s != nil {
				t.Fatalf("accepted %s: %v", mode, err)
			}
		})
	}
}
