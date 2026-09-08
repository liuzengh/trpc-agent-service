// usage_meter.go turns one agent turn's consumption into idempotent
// usage_records entries. Every metered dimension derives its RecordID from the
// inbound message id plus the dimension (usageRecordID), so a redelivered turn
// never double-counts. Dimensions: token (summed model usage), tool (any tool
// invocation), sandbox (code-exec tool invocations), artifact (files persisted
// via the artifact service), skill (skills whose SKILL.md was actually
// injected this turn — only those that loaded, not every mounted id).
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sort"
	"sync/atomic"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// codeExecToolName is the registered name of the sandboxed code-execution
// tool (ID "code-exec", name "execute_code", registered in cmd/trpc-service).
// Invocations of it meter the "sandbox" usage dimension.
const codeExecToolName = "execute_code"

// usageRecordID derives the idempotent usage-record key for one (message,
// dimension) pair. It is a bounded hash rather than "messageID:dimension"
// verbatim: usage_records.record_id is VARCHAR(36) and a 36-char UUID message
// id (admin console chat) plus a dimension suffix would overflow, truncate
// back to the bare id, and collide with the token record — INSERT IGNORE
// would then silently drop the tool/skill/sandbox/artifact rows. The hash is
// deterministic, so a redelivered turn still maps to the same key.
func usageRecordID(msgID, dim string) string {
	sum := sha256.Sum256([]byte(msgID + ":" + dim))
	return hex.EncodeToString(sum[:16]) // 32 hex chars: always fits the column
}

// skillUsageRef is the human-readable snapshot of one injected skill stored in
// the "skill" usage dimension's meta. It is a snapshot on purpose: a skill may
// be unpublished or renamed later, and the usage history must stay readable.
type skillUsageRef struct {
	SkillID string `json:"skill_id"`
	Code    string `json:"code"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// buildUsageEntries aggregates a finished turn into metered usage entries.
// token is recorded only when > 0; every other dimension is recorded only
// when its amount is positive. meta carries human-readable context for the
// usage/audit views: the tool dimension records the distinct tool names plus
// per-tool call counts; the skill dimension records skill snapshots (id, code,
// name, version) so the front-end can show exactly which skills ran.
func buildUsageEntries(m *bus.Message, agentID string, tokens int64, toolCalls map[string]int, skills []skillUsageRef, artifactSaves int32) []audit.UsageEntry {

	if m == nil {
		return nil
	}
	var out []audit.UsageEntry

	appendDim := func(dim string, amount float64, meta map[string]any) {
		out = append(out, audit.UsageEntry{
			RecordID:  usageRecordID(m.ID, dim),
			TenantID:  m.TenantID,
			AgentID:   agentID,
			Dimension: dim,
			Amount:    amount,
			Meta:      meta,
		})
	}

	if tokens > 0 {
		appendDim(audit.UsageDimensionToken, float64(tokens), nil)
	}

	var toolTotal int
	var toolNames []string
	for name, n := range toolCalls {
		toolTotal += n
		if n > 0 {
			toolNames = append(toolNames, name)
		}
	}
	sort.Strings(toolNames) // deterministic Meta ordering for stable audit rows
	if toolTotal > 0 {
		appendDim(audit.UsageDimensionTool, float64(toolTotal), map[string]any{"tools": toolNames, "calls": toolCalls})
	}
	if sand := toolCalls[codeExecToolName]; sand > 0 {
		appendDim(audit.UsageDimensionSandbox, float64(sand), nil)
	}
	if artifactSaves > 0 {
		appendDim(audit.UsageDimensionArtifact, float64(artifactSaves), nil)
	}
	if len(skills) > 0 {
		appendDim(audit.UsageDimensionSkill, float64(len(skills)), map[string]any{"skills": skills})
	}
	return out
}

// artifactUsage wraps an artifact.Service to count successful saves, so the
// "artifact" usage dimension has a real production trigger. All methods
// forward to the inner service.
type artifactUsage struct {
	inner artifact.Service
	saves atomic.Int64
}

func (a *artifactUsage) SaveArtifact(ctx context.Context, si artifact.SessionInfo, filename string, art *artifact.Artifact) (int, error) {
	rev, err := a.inner.SaveArtifact(ctx, si, filename, art)
	if err == nil {
		a.saves.Add(1)
	}
	return rev, err
}

func (a *artifactUsage) LoadArtifact(ctx context.Context, si artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	return a.inner.LoadArtifact(ctx, si, filename, version)
}

func (a *artifactUsage) ListArtifactKeys(ctx context.Context, si artifact.SessionInfo) ([]string, error) {
	return a.inner.ListArtifactKeys(ctx, si)
}

func (a *artifactUsage) DeleteArtifact(ctx context.Context, si artifact.SessionInfo, filename string) error {
	return a.inner.DeleteArtifact(ctx, si, filename)
}

func (a *artifactUsage) ListVersions(ctx context.Context, si artifact.SessionInfo, filename string) ([]int, error) {
	return a.inner.ListVersions(ctx, si, filename)
}

func (a *artifactUsage) count() int32 {
	return int32(a.saves.Load())
}

// recordUsage writes the metered usage entries of one turn, best-effort and
// idempotent (INSERT IGNORE on the derived record_id). A metering failure is
// logged and never fails or retries the reply flow.
func (w *Worker) recordUsage(ctx context.Context, m *bus.Message, agentID string, entries []audit.UsageEntry) {
	if w.auditor == nil || m == nil {
		return
	}
	for _, e := range entries {
		if err := w.auditor.RecordUsage(ctx, e); err != nil {
			slog.Warn("worker: usage metering skipped (best-effort)",
				"session", m.SessionID, "dimension", e.Dimension, "err", err)
		}
	}
}
