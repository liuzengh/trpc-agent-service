package governance

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type DLPVerdict string

const (
	DLPVerdictClean    DLPVerdict = "clean"
	DLPVerdictRejected DLPVerdict = "rejected"
	DLPVerdictUnknown  DLPVerdict = "unknown"
)

type ContentGuard interface {
	Inspect(context.Context, string, string, string, []byte) (DLPVerdict, string, error)
}

type EvaluationInput struct {
	TenantID      string
	RequestID     string
	Stage         string
	SubjectID     string
	PolicyVersion int64
	Model         VersionedRef
	DLP           DLPVerdict
}

type Evaluator struct{}

func (Evaluator) Evaluate(policy PolicySnapshot, in EvaluationInput) (Decision, error) {
	if policy.TenantID == "" || policy.TenantID != in.TenantID || policy.Version != in.PolicyVersion || in.RequestID == "" || in.Stage == "" || in.SubjectID == "" {
		return Decision{}, runtime.ErrTenantScope
	}
	decision := Decision{DecisionID: StableDecisionID(in.TenantID, in.RequestID, in.Stage, policy.Version), TenantID: in.TenantID, RequestID: in.RequestID,
		Stage: in.Stage, Action: ActionDeny, PolicyVersion: policy.Version}
	if in.DLP != DLPVerdictClean {
		decision.ReasonCode = ReasonInputRejected
		return decision, nil
	}
	if !ModelAllowed(policy, in.Model) {
		decision.ReasonCode = ReasonModelDenied
		return decision, nil
	}
	if policy.Policy.DefaultAction != ActionAllow {
		decision.ReasonCode = ReasonSubjectDenied
		return decision, nil
	}
	decision.Action, decision.ReasonCode = ActionAllow, ReasonAllowed
	return decision, nil
}

func StableDecisionID(tenantID, requestID, stage string, version int64) string {
	return "gdec_" + contentDigest([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", tenantID, requestID, stage, version)))[:32]
}

func ToolDecision(policy PolicySnapshot, tool VersionedRef) Decision {
	decision := Decision{DecisionID: StableDecisionID(policy.TenantID, tool.ID, "tool", policy.Version), TenantID: policy.TenantID, Stage: "tool", Action: ActionDeny, ReasonCode: ReasonToolDenied, PolicyVersion: policy.Version}
	for _, rule := range policy.Policy.Tools {
		if rule.ToolID != tool.ID || rule.Version != tool.Version {
			continue
		}
		if rule.Dangerous {
			if rule.ConfirmationSupported {
				decision.Action, decision.ReasonCode = ActionAsk, ReasonConfirmationRequired
				return decision
			}
			decision.ReasonCode = ReasonConfirmationMissing
			return decision
		}
		decision.Action, decision.ReasonCode = ActionAllow, ReasonAllowed
		return decision
	}
	return decision
}
